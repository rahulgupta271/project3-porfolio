#!/usr/bin/env python
# coding: utf-8

# # 5.2 Programming Exercise: Create a Small Data Warehouse

# In this exercise, you will create a small data warehouse using Spark to save data as if it were a table in a typical relational database. Once you create this data warehouse, you can query the tables you created using Structured Query Language (SQL).
# 
# For this exercise, you will execute your Spark SQL within a Python program, but if you are using a typical Hadoop distribution, there are many ways you can connect those tables to existing tools as if it were a normal, relational database. Spark SQL natively supports reading and writing data managed by Apache Hive. Spark can act as a distributed SQL engine allowing you to connect to any tool with JDBC/ODBC support. You can also integrate Spark with big data tools like Apache Phoenix and normal relational databases.
# 
# For this exercise, you will be creating tables using U.S. Gazetteer files provided by the United States Census Bureau. These files provide a listing of geographic areas for selected areas. You can find the Gazetteer files for 2017 and 2018 in the data directory under the gazetteer folder. These directories contain data for congressional districts, core statistical areas, counties, county subdivisions, schools, census tracts, urban areas, zip codes, and places of interest. You will combine the data from 2017 and 2018, and create tables with the filename of the source (e.g., places.csv is saved in the places table).
# 
# 

# # 1. Gazetteer Data

# ## a. Create Unmanaged Tables

# 
# 
# The first step of this assignment involves loading the data from the CSV files, combining the file with the file for the other year, and saving it to disk as a table. The following code should provide a template to help you combine tables and save them to the warehouse directory. 

# In[1]:


from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))
#import pandas


# In[2]:


# Create file paths including filenames
file_2017_path = r'/home/ram/share/650/dsc650-master/data/gazetteer/2017/places.csv'

file_2018_path = r'/home/ram/share/650/dsc650-master/data/gazetteer/2018/places.csv'

warehouse_dir = r'/home/ram/Documents/spark-warehouse/'


# In[3]:


spark = SparkSession.builder.appName('week5')         .config("spark.sql.warehouse.dir", warehouse_dir)         .getOrCreate()


# In[4]:


df1 = spark.read.load(
  file_2017_path,
  format='csv',
  sep=',',
  inferSchema=True,
  header=True
)

df2 = spark.read.load(
  file_2018_path,
  format='csv',
  sep=',',
  inferSchema=True,
  header=True
)


# In[5]:


df = df1.unionAll(df2)
df.head(3)


# In[6]:


df.write.saveAsTable('places', path = warehouse_dir)


# In[7]:


#df.write.saveAsTable('places', mode = 'overwrite', path = '/home/ram/share/650/spark-warehouse/')


# For each CSV file in the 2017 and 2018 directories, load the data into Spark, combine it with the corresponding data from the other year and save it to disk.

# In[8]:


import os

files_2017_path = r'/home/ram/share/650/dsc650-master/data/gazetteer/2017'

files_2018_path = r'/home/ram/share/650/dsc650-master/data/gazetteer/2018'

#os.listdir("/home/ram/share/650/dsc650-master/data/gazetteer/2017")
fileslist_2017 = os.listdir(files_2017_path)
fileslist_2018 = os.listdir(files_2018_path)


# In[9]:


# getting list of files in the directory
import glob
print(glob.glob("/home/ram/share/650/dsc650-master/data/gazetteer/2017/*.csv"))


# In[10]:


# list of files in 2017 directory
fileslist_2017


# In[11]:


# list of files in 2018 directory
fileslist_2018


# In[12]:


def saveastable(file, warehouse_dir):
    """
    Combines csv files from different directories (2017 & 2018) and creates a spark table
    inputs: 2 - filename, warehouse directory
    output: none
    expectation: combines files and creates parqurt tables warehouse directory
    """
    
    file1_path = os.path.join(files_2017_path,file)
    file2_path = os.path.join(files_2018_path,file)
    df1 = spark.read.load(
      file1_path,
      format='csv',
      sep=',',
      inferSchema=True,
      header=True
    )

    df2 = spark.read.load(
      file2_path,
      format='csv',
      sep=',',
      inferSchema=True,
      header=True
    )

    df = df1.unionAll(df2)
    
    tablename = os.path.splitext(i)[0]
    tblwarehouse_dir = os.path.join(warehouse_dir,tablename)
    df.write.saveAsTable(tablename, mode = 'overwrite', path = tblwarehouse_dir )
    print(" Table created for - ",tablename)


# In[13]:


# loops through each file and merges the 2017 & 2018 data for same file and saves in warehouse directory
table_names = []
for i in fileslist_2017:
    filename = i
    tablename = os.path.splitext(i)[0]
    table_names.append(tablename)
    file_path_name = os.path.join(files_2017_path,i)
    print(filename)    
    print(tablename)
    print(file_path_name)
    #call function to merge and create unified parquet file and save it in warehouse directory
    saveastable(filename, warehouse_dir)
        


# Once you have finished saving all of the files as tables, verify that you have loaded the files properly by loading the tables into Spark, and performing a simple row count on each table.

# In[14]:


# I did not use this code, as I have already created tables in the above step
def create_external_table(table_name):
    table_dir = os.path.join(warehouse_dir, table_name)
    return spark.catalog.createExternalTable(table_name, table_dir)

def create_external_tables():
    for table_name in table_names:
        create_external_table(table_name)


# In[15]:


# show existing tables
table_names

#for i in tablelist:
#    create_external_tables() 


# In[16]:


#Getting list of existing tables in spark warehouse
spark.catalog.listTables()


# In[17]:


spark.sql("select count(*) as row_count from places").show()


# In[18]:


# printing count of rows from each of the merged tables
for i in table_names:
    sqlstring = "select count(*) as row_count from " + i
    print("Count of rows from table - ", i)
    spark.sql(sqlstring).show()


# ## b. Load and Query Tables

# 
# Now that we have saved the data to external tables, we will load the tables back into Spark and create a report using Spark SQL. For this report, we will create a report on school districts for the states of Nebraska and Iowa using the elementary_schools, secondary_schools and unified_school_districts tables. 
# 
# Using Spark SQL, create a report with the following information.

# In[19]:


spark.sql("select * from elementary_schools").show(3)


# In[20]:


spark.sql("select * from secondary_schools").show(3)


# In[21]:


spark.sql("select * from unified_school_districts").show(3)


# In[22]:


# reading spark tables and printing schema
elementary_schools_df = spark.read.table("elementary_schools")
print("Number of rows ", elementary_schools_df.count())
elementary_schools_df.printSchema()


# In[23]:


# reading spark tables and printing schema
secondary_schools_df = spark.read.table("secondary_schools")
print("Number of rows ", secondary_schools_df.count())
secondary_schools_df.printSchema()


# In[24]:


# reading spark tables and printing schema
unified_school_districts_df = spark.read.table("unified_school_districts")
print("Number of rows ", unified_school_districts_df.count())
unified_school_districts_df.printSchema()


# es_ilne = spark.sql("""select state, year, count(*) as Elementary 
#                                     from elementary_schools 
#                                     where state = 'IA' or state = 'NE'
#                                     or state = 'NJ'  or state = 'IL'
#                                     group by state, year""").collect()

# I have joined whole data set based on state and year and not just for those 2 states, as Elementary and Seconday school data sets do not have data for these states.

# In[25]:


es_ilne = spark.sql("""select state, year, count(*) as Elementary 
                                    from elementary_schools 
                                     group by state, year""").collect()

#converting list to data frame
es_ilne_df = sc.parallelize(es_ilne).toDF()
es_ilne_df.printSchema()


# In[26]:


es_ilne_df.show()


# In[27]:


ss_ilne = spark.sql("""select state, year, count(*) as Secondary 
                                    from secondary_schools 
                                     group by state, year""").collect()
#converting list to data frame
ss_ilne_df = sc.parallelize(ss_ilne).toDF()
ss_ilne_df.printSchema()


# In[28]:


ss_ilne_df.show()


# In[29]:


usd_ilne = spark.sql("""select state, year, count(*) as Unified 
                                    from unified_school_districts 
                                    group by state, year""").collect()

#converting list to data frame
usd_ilne_df = sc.parallelize(usd_ilne).toDF()
usd_ilne_df.printSchema()


# In[30]:


# renaming columns to preserve for later stages
usd_ilne_df = usd_ilne_df.withColumnRenamed("state","JState")                        .withColumnRenamed("year","JYear")
usd_ilne_df.show()


# In[31]:


joinexpression =  [usd_ilne_df.JState == es_ilne_df.state, usd_ilne_df.JYear == es_ilne_df.year]
joinType = "left_outer"


# In[32]:


reportdf1 = usd_ilne_df.join(es_ilne_df,joinexpression,joinType)#.collect()
reportdf1 = reportdf1.drop("state", "year")
reportdf1.show()


# In[33]:


joinexpression =  [reportdf1.JState == ss_ilne_df.state, reportdf1.JYear == ss_ilne_df.year]
joinType = "left_outer"


# In[34]:


reportdf = reportdf1.join(ss_ilne_df,joinexpression,joinType)#.collect()
reportdf = reportdf.drop("state", "year")

reportdf = reportdf.withColumnRenamed("JState","State")                        .withColumnRenamed("JYear","Year")

reportdf = reportdf.select("State", "Year", "Elementary", "Secondary", "Unified")                    .sort("State", "Year")
reportdf.show()


# In[35]:


# saving the summarized report for all states as spark table in warehouse
tablename = 'Allstates_counts'
tblwarehouse_dir = os.path.join(warehouse_dir,tablename)
reportdf.write.saveAsTable(tablename, mode = 'overwrite', path = tblwarehouse_dir )


# In[41]:


# using spark SQL analyse the table for few states
summary_fewstates = spark.sql("""select *
                                    from Allstates_counts 
                                    where state In ('NE' ,'IA') 
                                    order by state""")

#print the report output for states IA & NE
summary_fewstates.show()


# # 2. Flight Data
# 

# 
# In the previous exercise, you joined data from flights and airport codes to create a report. Create an external table for airport_codes and domestic_flights from the domestic-flights/flights.parquet and airport-codes/airport-codes.csv files. Recreate the report of top ten airports for 2008 using Spark SQL instead of dataframes.

# In[42]:


# Create file paths including filenames
parquet_file_path = r'/home/ram/share/650/dsc650-master/data/domestic-flights/flights.parquet'

airportdata_filepath = r'/home/ram/share/650/dsc650-master/data/airport-codes/airport-codes.csv'


# In[43]:


df_flight = spark.read.parquet(parquet_file_path)
df_flight.head(5)


# In[44]:


df_airpot_codes = spark.read.load(airportdata_filepath, format="csv", sep=",", inferschema=True, header=True)

df_airpot_codes.head(5)


# ## Join to Origin Airport

# In[45]:


joinexpression =  df_flight['origin_airport_code'] == df_airpot_codes['iata_code']
joinType = "left_outer"


# In[46]:


#df_flight.join(df_airpot_codes,joinexpression,joinType).show(3)
df_merged = df_flight.join(df_airpot_codes,joinexpression,joinType)
df_merged_modified = df_merged.drop("__index_level_0__","ident","local_code","continent","iso_country","iata_code")
df_merged_modified.head(2)


# In[47]:


df_merged_modified2 = df_merged_modified.withColumnRenamed("type","origin_airport_type")                                        .withColumnRenamed("name","origin_airport_name")                                        .withColumnRenamed("elevation_ft","origin_airport_elevation_ft")                                        .withColumnRenamed("iso_region","origin_airport_region")                                        .withColumnRenamed("municipality","origin_airport_municipality")                                        .withColumnRenamed("gps_code","origin_airport_gps_code")                                        .withColumnRenamed("coordinates","origin_airport_coordinates")


# In[48]:


df_merged_modified2.printSchema()


# ## Join to Destination Airport

# In[49]:


joinexpression2 =  df_merged_modified2['destination_airport_code'] == df_airpot_codes['iata_code']
joinType2 = "left_outer"


# In[50]:


#df_merged_modified2.join(df_airpot_codes,joinexpression2,joinType2).show(2)
df_merged_modified_dest= df_merged_modified2.join(df_airpot_codes,joinexpression2,joinType2)

df_merged_modified_dest2 = df_merged_modified_dest.drop("__index_level_0__","ident","local_code","continent","iso_country")


df_merged_modified_dest_final = df_merged_modified_dest2.withColumnRenamed("type","destination_airport_type")                                        .withColumnRenamed("name","destination_airport_name")                                        .withColumnRenamed("elevation_ft","destination_airport_elevation_ft")                                        .withColumnRenamed("iso_region","destination_airport_region")                                        .withColumnRenamed("municipality","destination_airport_municipality")                                        .withColumnRenamed("gps_code","destination_airport_gps_code")                                        .withColumnRenamed("coordinates","destination_airport_coordinates")


# In[51]:


df_merged_modified_dest_final.printSchema()


# ## Top Ten Airports By Inbound Passengers

# In[52]:


df_merged_modified_dest_final.createOrReplaceTempView("dfTable")


# In[53]:


# dataframe with data from 2008
df_2008 = spark.sql("SELECT * FROM dfTable where flight_year = 2008")
df_2008.head(2)


# In[54]:


# saving dataframe with 2008 as a table
df_2008.write.saveAsTable('Flight_2008_data', mode = 'overwrite', 
                          path = '/home/ram/Documents/spark-warehouse/flight_data')


# In[55]:


# dataframe with data from 2008
Top10 = spark.sql(""" SELECT * FROM 
                    (
                    SELECT  Airport_Name,
                            Airpot_Code,
                            dense_rank() Over(ORDER BY Total_Inbound_Passengers DESC) as Rank,
                            Total_Inbound_Passengers,
                            Total_Inbound_Flights,
                            Average_Daily_Passengers,
                            Average_DailyFlights
                        FROM 
                        (
                            SELECT 
                            destination_airport_name as Airport_Name, 
                            destination_airport_code as Airpot_Code,  
                            count(passengers) as Total_Inbound_Passengers,
                            count(flights) as Total_Inbound_Flights,
                            mean(passengers) as Average_Daily_Passengers,
                            mean(flights) as Average_DailyFlights                            
                            FROM Flight_2008_data 
                            GROUP BY destination_airport_name, destination_airport_code
                            ) Temp
                      ) RankedTable 
                          WHERE Rank < 11
                """)


# In[56]:


Top10.show()


# In[ ]:




