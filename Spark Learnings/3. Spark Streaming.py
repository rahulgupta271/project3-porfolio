#!/usr/bin/env python
# coding: utf-8

# # 1. Stream Directory Data
# 

# 
# In the first part of the exercise, you will create a simple Spark streaming program that reads an input stream from a file source. The file source stream reader reads data from a directory on a file system. When a new file is added to the folder, Spark adds that file’s data to the input data stream.
# 
# You can find the input data for this exercise in the baby-names/streaming directory. This directory contains the baby names CSV file randomized and split into 98 individual files. You will use these files to simulate incoming streaming data.
# 
# 

# ## a. Count the Number of Females
# 
# 

# In the first part of the exercise, you will create a Spark program that monitors an incoming directory. To simulate streaming data, you will copy CSV files from the baby-names/streaming directory into the incoming directory. Since you will be loading CSV data, you will need to define a schema before you initialize the streaming dataframe.
# 
# From this input data stream, you will create a simple output data stream that counts the number of females and writes it to the console. Approximately every 10 seconds or so, copy a new file into the directory and report the console output. Do this for the first ten files.

# In[1]:


# Import required modules
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

import shutil 
from time import sleep
import os.path 
import os


# In[36]:


# Create file paths including filenames
file_path = r'/home/ram/share/650/dsc650-master/data/baby-names/streaming'

ipstreaming_file_path = r'/home/ram/Documents/650/input_streaming'

batchstream_file_path = r'/home/ram/Documents/650/batch_streaming'


# In[3]:


spark = SparkSession.builder.appName('week8')         .getOrCreate()


# In[4]:


static = spark.read.csv(file_path, header = True)
dataschema = static.schema


# In[5]:


static.printSchema()


# In[6]:


static.head(5)


# In[7]:


static.count()
#5880000


# In[8]:


print(type(static))


# In[9]:


# reading stream from input directory
streaming = spark.readStream.schema(dataschema).csv(ipstreaming_file_path)   


# In[10]:


#.option("maxFilesPerTrigger", 1)\


# In[11]:


counts = streaming.groupBy("sex").count()
counts


# counts.select("sex").where("sex = 'F'")
# counts.groupBy("sex").count()

# In[27]:


streamingquery.stop()


# In[29]:


streamingquery = counts.writeStream.queryName("counts")                                    .format("memory").outputMode("complete")                                    .start()


# In[30]:


#print active streams
spark.streams.active


# In[15]:


print(static.isStreaming)


# In[59]:


from time import sleep

for x in range(5):
    spark.sql("SELECT * FROM counts").show()
    sleep(1)


# In[16]:




fileslist = os.listdir(file_path)
print(fileslist[1:11])


# In[50]:




src_path = os.path.join(file_path,fileslist[4])

dest_path = os.path.join(ipstreaming_file_path,fileslist[4])

print(src_path, dest_path)


# In[32]:


for i in range(len(fileslist[1:11])):
    
    file = fileslist[i]
    
    print(file)
    src_path = os.path.join(file_path,file)
    dest_path = os.path.join(ipstreaming_file_path,file)
    
    shutil.copy(src_path, dest_path) 
    print("Moved the file \n")
    
    
    print("Lets check the counts \n ")
    sleep(2)
    spark.sql("SELECT * FROM counts").show()
    sleep(2)


# ## 2. Micro-Batching
# 
# Repeat the last step, but use a micro-batch interval to trigger the processing every 30 seconds. Approximately every 10 seconds or so, copy a new file into the directory and report the console output. Do this for the first ten files. How did the output differ from the previous example?

# In[41]:


from pyspark.sql.functions import window


# In[39]:


# reading stream from input directory
csvdf = spark.readStream.schema(dataschema).csv(batchstream_file_path)  

batch_counts = csvdf.groupBy("sex").count()

#select count of females

batch_counts.select("sex").where("sex = 'F'")

batch_counts.groupby("sex").count()


# In[44]:


#microbatch_counts = csvdf.groupBy(window("30 seconds"), csvdf.sex).count()


# In[45]:


microbatch_writer = batch_counts.writeStream.trigger(processingTime = '30 seconds').queryName("batch_counts")                                    .format("memory").outputMode("complete")                                    .start()


# In[46]:


microbatch_writer.isActive


# In[48]:


for i in range(len(fileslist[1:11])):
    
    file = fileslist[i]
    
    print(file)
    src_path = os.path.join(file_path,file)
    dest_path = os.path.join(batchstream_file_path,file)
    
    shutil.copy(src_path, dest_path) 
    print("Moved the file \n")
    
    
    print("Lets check the counts \n ")
    sleep(1)
    spark.sql("SELECT * FROM batch_counts").show()
    sleep(10)


# In[ ]:




