#!/usr/bin/env python
# coding: utf-8

# # Flight Data

# In the first part of this exercise, you will load flight data from the domestic-flights/flights.parquet file and airport codes from the airport-codes/airport-codes.csv file. As a reminder, you can load a parquet and CSV files as follows. (Click on the image below to download the code.)
# 
# As a first step, load both files into Spark and print the schemas. The flight data uses the International Air Transport Association (IATA) codes of the origin and destination airports. The IATA code is a three-letter code identifying the airport. For instance, Omaha’s Eppley Airfield is OMA, Baltimore-Washington International Airport is BWI, Los Angeles International Airport is LAX, and New York’s John F. Kennedy International Airport is JFK. The airport codes file contains information for each of the airports.

# Import Requred modules

# In[1]:


from pyspark.sql import SparkSession
import pandas

spark = SparkSession.builder.appName('week4').getOrCreate()


# In[2]:


# Create file paths including filenames
parquet_file_path = r'/home/ram/share/650/dsc650-master/data/domestic-flights/flights.parquet'

airportdata_filepath = r'/home/ram/share/650/dsc650-master/data/airport-codes/airport-codes.csv'


# In[3]:


df_flight = spark.read.parquet(parquet_file_path)


# In[4]:


df_flight.head(5)


# In[5]:


df_airpot_codes = spark.read.load(airportdata_filepath, format="csv", sep=",", inferschema=True, header=True)

df_airpot_codes.head(5)


# In[6]:


#printing schemas
df_flight.printSchema()


# In[7]:


df_airpot_codes.printSchema()


# # Join Data

# Join the flight data to airport codes data by matching the IATA code of the originating flight to the IATA code in the airport codes file. Note that the airport codes file may not contain IATA codes for all of the origin and destination flights in the flight data. We still want information on those flights even if we cannot match it to a value in the airport codes file. This means you will want to use a left join instead of the default inner join.

# In[8]:


joinexpression =  df_flight['origin_airport_code'] == df_airpot_codes['iata_code']
joinType = "left_outer"


# In[136]:


df_flight.join(df_airpot_codes,joinexpression,joinType).show(3)


# In[10]:


df_merged = df_flight.join(df_airpot_codes,joinexpression,joinType)


# In[135]:


df_merged.head(2)


# In[12]:


df_merged.printSchema()


# # Rename and Remove columns

# Next, we want to rename some of the joined columns and remove unneeded columns. Remove the following columns from the joined dataframe.
# 
#     __index_level_0__
#     ident
#     local_code
#     continent
#     iso_country
#     iata_code
# 
# Rename the following columns.
# 
#     type: origin_airport_type
#     name: origin_airport_name
#     elevation_ft: origin_airport_elevation_ft
#     iso_region: origin_airport_region
#     municipality: origin_airport_municipality
#     gps_code: origin_airport_gps_code
#     coordinates: origin_airport_coordinates
# 

# In[21]:


df_merged_modified = df_merged.drop("__index_level_0__","ident","local_code","continent","iso_country","iata_code")


# In[22]:


df_merged.printSchema()


# In[24]:


df_merged_modified.printSchema()


# In[28]:


#df_merged_modified2 = df_merged_modified.withColumnRenamed("type","origin_airport_type").withColumnRenamed("name","origin_airport_name").withColumnRenamed("elevation_ft","origin_airport_elevation_ft").withColumnRenamed("iso_region","origin_airport_region").withColumnRenamed("municipality","origin_airport_municipality").withColumnRenamed("gps_code","origin_airport_gps_code").withColumnRenamed("coordinates","origin_airport_coordinates")


# In[32]:


df_merged_modified2 = df_merged_modified.withColumnRenamed("type","origin_airport_type")                                        .withColumnRenamed("name","origin_airport_name")                                        .withColumnRenamed("elevation_ft","origin_airport_elevation_ft")                                        .withColumnRenamed("iso_region","origin_airport_region")                                        .withColumnRenamed("municipality","origin_airport_municipality")                                        .withColumnRenamed("gps_code","origin_airport_gps_code")                                        .withColumnRenamed("coordinates","origin_airport_coordinates")


# In[33]:


df_merged_modified2.printSchema()


# # Join to Destination Airport

# Repeat parts a and b joining the airport codes file to the destination airport instead of the origin airport. Drop the same columns and rename the same columns using the prefix destination_airport_ instead of origin_airport_. Print the schema of the resultant dataframe. The final schema and dataframe should contain the added information (name, region, coordinate, …) for the destination and origin airports.

# In[106]:


joinexpression2 =  df_merged_modified2['destination_airport_code'] == df_airpot_codes['iata_code']
joinType2 = "left_outer"


# In[108]:


df_merged_modified2.join(df_airpot_codes,joinexpression2,joinType2).show(2)


# In[109]:


df_merged_modified_dest= df_merged_modified2.join(df_airpot_codes,joinexpression2,joinType2)


# In[110]:


df_merged_modified_dest.printSchema()


# In[111]:



df_merged_modified_dest2 = df_merged_modified_dest.drop("__index_level_0__","ident","local_code","continent","iso_country")


# In[112]:


df_merged_modified_dest2.printSchema()


# In[113]:


df_merged_modified_dest_final = df_merged_modified_dest2.withColumnRenamed("type","destination_airport_type")                                        .withColumnRenamed("name","destination_airport_name")                                        .withColumnRenamed("elevation_ft","destination_airport_elevation_ft")                                        .withColumnRenamed("iso_region","destination_airport_region")                                        .withColumnRenamed("municipality","destination_airport_municipality")                                        .withColumnRenamed("gps_code","destination_airport_gps_code")                                        .withColumnRenamed("coordinates","destination_airport_coordinates")


# In[114]:


df_merged_modified_dest_final.printSchema()


# In[115]:


df_merged_modified_dest_final.head(2)


# # Top Ten Airports

# Create a dataframe using only data from 2008. This dataframe will be a report of the top ten airports by the number of inbound passengers. This dataframe should contain the following fields:
# 
#     Rank (1-10)
#     Name
#     IATA code
#     Total Inbound Passengers
#     Total Inbound Flights
#     Average Daily Passengers
#     Average Inbound Flights
# 
# Show the results of this dataframe using the show method.

# In[116]:


df_merged_modified_dest_final.createOrReplaceTempView("dfTable")


# In[117]:


sqlcount = spark.sql("SELECT COUNT(*) FROM dfTable")

sqlcount.show()


# In[120]:


# dataframe with data from 2008
df_2008 = spark.sql("SELECT * FROM dfTable where flight_year = 2008")
df_2008.head(2)


# In[121]:


df_2008.printSchema()


# In[122]:


display(df_2008.head(2))


# In[123]:


from pyspark.sql.functions import *
from pyspark.sql.window import Window


# In[124]:


df_2008.groupBy("destination_airport_name","iata_code")        .agg(count("passengers").alias("Total Inbound Passengers"),            count("flights").alias("Total Inbound Flights"),            mean("passengers").alias("Total Inbound Passengers"),            mean("flights").alias("Total Inbound Flights"),            ).show()


# In[125]:


df_2008_temp = df_2008.groupBy("destination_airport_name","iata_code")        .agg(count("passengers").alias("Total Inbound Passengers"),            countDistinct("flights").alias("Total Inbound Flights"),            mean("passengers").alias("Average Daily Passengers"),            mean("flights").alias("Average Daily Flights"),            )


# In[126]:


df_2008_rank = df_2008_temp        .withColumn('Rank',dense_rank().over(Window.orderBy(desc('Total Inbound Passengers'))))        .withColumnRenamed('destination_airport_name', 'Name')        .withColumnRenamed('iata_code', 'IATA code')        .filter(col('Rank') <=10)


# In[127]:


df_2008_rank.show()


# # User Defined Functions

# The latitude and longitude coordinates for the destination and origin airports are string values and not numeric. You will create a user-defined function in Python that will convert the string coordinates into numeric coordinates. Below is the Python code that will help you create and use this user-defined function. (Click on the image below to download the code.)

# In[128]:


from pyspark.sql.functions import udf


# In[129]:


@udf('double')
def get_latitude(coordinates):
    split_coords = coordinates.split(',')
    if len(split_coords) != 2:
        return None

    return float(split_coords[0].strip())


@udf('double')
def get_longitude(coordinates):
    split_coords = coordinates.split(',')
    if len(split_coords) != 2:
        return None

    return float(split_coords[1].strip())


# In[133]:


df__final = df_merged_modified_dest_final.withColumn('origin_airport_latitude',get_latitude(df_merged_modified_dest_final['origin_airport_coordinates'])).withColumn('origin_airport_longitude',get_longitude(df_merged_modified_dest_final['origin_airport_coordinates'])).withColumn('destination_airport_latitude',get_latitude(df_merged_modified_dest_final['destination_airport_coordinates'])).withColumn('destination_airport_longitude',get_longitude(df_merged_modified_dest_final['destination_airport_coordinates']))


# In[134]:


df__final.head(2)


# In[ ]:




