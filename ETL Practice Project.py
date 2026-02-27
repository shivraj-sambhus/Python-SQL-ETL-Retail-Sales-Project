import pandas as pd

#This part was when I first took a look at the data. The project begins on line 16.
#sales_df = pd.read_csv("Spring Skillup/LA_Retail_Sales.csv")

#print(sales_df.head())

#print(sales_df.info())

import glob

#file_paths = glob.glob("Spring Skillup/LA_Retail_Sales_By_Day/*.csv")

#sales_df = pd.concat([pd.read_csv(fp) for fp in file_paths], ignore_index=True)

#EXTRACT PHASE START
#Turning daily retail sales data into a combined dataframe
sales_df = pd.DataFrame()

for day in range(1,31):
    #print(day)
    day_df = pd.read_csv(f"Spring Skillup/LA_Retail_Sales_By_Day/sales_2024-09-{day:02d}.csv")
    sales_df = pd.concat([sales_df,day_df])
    #print(len(sales_df))

#print(sales_df.head())

#Reading in the SQLite file
import sqlite3
conn = sqlite3.connect("Spring Skillup/la_sales.sqlite")
pd.read_sql("SELECT * FROM Sales", con=conn)

#Weather affects our analysis. Read in weather data using an API
import requests
import time 
from datetime import datetime

#Create each date in the sales data to a datetime object
sales_df["date"] = pd.to_datetime(sales_df["date"])

#unix_timestamp = int(sales_df["date"].iloc[0].timestamp())

#url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat=34.0522&lon=-118.2437&dt={unix_timestamp}&appid=05b9b40d9abc6964821a8c28ad5fad68&units=imperial"

#response = requests.get(url) #Calls the API key

#data = response.json() #Contains the weather data in LA at the given date


# A list of weather data to be populated by the FOR loop below
weather_data = []

# Cleaning up the date column from the sales dataframe
dates = sales_df.date.sort_values().dropna().unique()

for date in dates:
    # The unix timestamp for each day of the month
    unix_timestamp = int(pd.Timestamp(date).timestamp()) 
    
    # The OpenWeatherMap API from which we pull the weather at each day
    url = f"https://api.openweathermap.org/data/3.0/onecall/timemachine?lat=34.0522&lon=-118.2437&dt={unix_timestamp}&appid=05b9b40d9abc6964821a8c28ad5fad68&units=imperial"
    
    # The weather data itself for the date
    response = requests.get(url)
    
    # Conditional loop that populates the weather_data list
    if response.status_code == 200:
        data = response.json()
        day_weather = {
            "date": pd.to_datetime(date).date(),
            "temp": data["data"][0]["temp"],
            "humidity": data["data"][0]["humidity"],
            "weather_main": data["data"][0]["weather"][0]["main"],
            "weather_desc": data["data"][0]["weather"][0]["description"]
        }
        weather_data.append(day_weather)
        print(f"Collected data for: {pd.to_datetime(date).date()}")
    else: 
        print(f"Failed to fetch data for {date}: {response.status_code}")
    
    # Respecting the API rate limit
    time.sleep(1) 

# The final dataframe
weather_df = pd.DataFrame(weather_data)

weather_df["date"] = pd.to_datetime(weather_df["date"])

#EXTRACT PHASE END
#TRANSFORM PHASE START

#Standardize the column names to all lower case without spaces
sales_df.columns = sales_df.columns.str.lower().str.replace(" ","_")

#print(sales_df.info())

#Clean the sales data by dropping missing values. I could instead replace them with 0 but, for now, I'll drop them.
sales_df = sales_df.dropna()

#print(sales_df.info())

#Create an additional column for the revenue per unit
sales_df["rev_per_unit"] = (sales_df["dollar_sales"] / sales_df["unit_sales"]).round(2)

#Some zip code entries have '900XX' instead of a full zip code. Replace the 'XX' with '00'
sales_df["store_zip"] = sales_df["store_zip"].str.replace('XX','00')

#Change the data type of the 'unit_sales' column to int
sales_df["unit_sales"] = sales_df["unit_sales"].astype(int)

#print(sales_df.info())

#Check for outliers in the sales dataframe. Seems fine.
#print(sales_df.describe())

#Check for outliers in the weather dataframe. Seems fine.
#print(weather_df.describe())

#Merge the sales dataframe and weather dataframe with a left join on the date of the sales
sales_df = sales_df.merge(weather_df[["date","temp", "humidity"]],how="left", on="date")

#print(sales_df.info())

#TRANSFORM PHASE END
#LOAD PHASE START

#Load into SQL so that we can query on the data

sales_df.to_sql("Sales", if_exists="replace", index=False, con=conn)

#A sample query 

print(pd.read_sql("SELECT * FROM Sales WHERE store_zip=9001 AND temp > 65", con=conn))

#LOAD PHASE END

# I want to create a scatterplot visualization of the temperature versus revenue. I don't want to do this for each store, so I'll pick the first one.

import seaborn as sns
import matplotlib.pyplot as plt

first_store_df = sales_df[sales_df["store_id"] == "LA001"]

#plt.figure(figsize=(10, 6))
#sns.scatterplot(data=first_store_df, x='rev_per_unit', y='temp')
#plt.title('Relationship between Revenue per Unit and Temperature')
#plt.xlabel('Revenue')
#plt.ylabel('Temperature (°F)')
#plt.show()

# Find the correlation between the temperature and the revenue. I want to see how related they are.

#corr = first_store_df['rev_per_unit'].corr(sales_df['temp'])

#print(corr)


plt.figure(figsize=(10, 6))

# We use 'hue' to represent the "heat" (Humidity)
# 'palette' sets the color theme (magma or viridis work well for "heat")
sns.scatterplot(
    data=first_store_df, 
    x='rev_per_unit', 
    y='temp', 
    hue='humidity', 
    palette='magma', 
    s=120, 
    edgecolor='black'
)

# 5. Final labeling and titles
plt.title('Revenue versus Temperature with heat for Humidity')
plt.xlabel('Revenue')
plt.ylabel('Temperature')

# Move the legend so it doesn't block the data
plt.legend(title='Humidity (%)', bbox_to_anchor=(1.05, 1), loc='upper left')

plt.tight_layout()
plt.show()
