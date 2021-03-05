#!/usr/bin/env python
# coding: utf-8

# # Fit a Binary Logistic Regression Model to a Dataset

# ## 1. Build a Classification Model
# 
# 

# In this exercise, you will fit a binary logistic regression model to the baby name dataset you used in the previous exercise. This model will predict the sex of a person based on their age, name, and state they were born in. To train the model, you will use the data found in baby-names/names-classifier.

# #### Importing data

# In[1]:


# Import required modules
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

spark = SparkSession.builder.appName('week10')         .getOrCreate()


# In[2]:


df = spark.read.parquet("/home/ram/share/650/dsc650-master/data/baby-names/names_classifier")
df.show()


# In[3]:


df.count()


# In[4]:


whole_df = df # taking backup of dataframe

distinct_df = df.distinct() # removing duplicates from df

df = distinct_df # copying distinct records into the main df


# In[5]:


df.count()


# ### a. Prepare in Input Features
# 

# First, you will need to prepare each of the input features. While age is a numeric feature, state and name are not. These need to be converted into numeric vectors before you can train the model. Use a StringIndexer along with the OneHotEncoderEstimator to convert the name, state, and sex columns into numeric vectors. Use the VectorAssembler to combine the name, state, and age vectors into a single features vector. Your final dataset should contain a column called features containing the prepared vector and a column called label containing the sex of the person.
# 
# 

#  #### Use a StringIndexer along with the OneHotEncoderEstimator to convert the name, state, and sex columns into numeric vectors

# In[6]:


name_indexer = StringIndexer(inputCol = "name", outputCol= "nameInd")
name_trsf = name_indexer.fit(df).transform(df) # transform(df.select("name"))
name_ohe = OneHotEncoder(inputCol = "nameInd", outputCol= "name_ohe")
name_featurevect = name_ohe.transform(name_trsf)


# In[7]:


name_featurevect


# In[8]:


state_indexer = StringIndexer(inputCol = "state", outputCol= "stateInd")
state_trsf = state_indexer.fit(name_featurevect).transform(name_featurevect) # transform(df.select("state"))
state_ohe= OneHotEncoder(inputCol = "stateInd", outputCol= "state_ohe")
state_featurevect = state_ohe.transform(state_trsf)


# In[9]:


state_featurevect


# In[10]:


state_featurevect.show()


# In[11]:


sex_indexer = StringIndexer(inputCol = "sex", outputCol= "sex_label")
sex_label = sex_indexer.fit(state_featurevect).transform(state_featurevect) # transform(df.select("sex"))
sex_ohe = OneHotEncoder(inputCol = "sex_label", outputCol= "sex_ohe_label")
features_label = sex_ohe.transform(sex_label)


# In[12]:


features_label


# In[13]:


features_label.show()


# In[14]:


df.columns


# #### Use the VectorAssembler to combine the name, state, and age vectors into a single features vector

# In[15]:


features_assembler = VectorAssembler(inputCols = ["name_ohe","state_ohe","age"],outputCol = "features")
assembler_op = features_assembler.transform(features_label)


# In[16]:


assembler_op


# In[17]:


final_dataset = assembler_op.select('features', 'sex_label')
final_dataset.columns


# In[18]:


final_dataset.show()


# ## 2. Fit and Evaluate the Model
# 

# Fit the model as a logistic regression model with the following parameters. LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8). Provide the area under the ROC curve for the model.

# In[19]:


lr = LogisticRegression(labelCol = 'sex_label',featuresCol = 'features', maxIter=10, regParam=0.3, elasticNetParam=0.8)
print(lr.explainParams())


# In[20]:


# test train split
train_data, test_data = final_dataset.randomSplit([0.7, 0.3], seed = 1)


# In[21]:


print("Training data set count : " + str(train_data.count()))
print("\n")
print("Test data set count : " + str(test_data.count()))


# In[22]:


# Fit the logistic regression model
lr_model = lr.fit(train_data)


# In[23]:


# print fitted logistic regression model summary
lr_model_summary = lr_model.summary


# In[24]:


# print the roc (of trained data) as a data frame 
lr_model_summary.roc.show(10)


# In[30]:


# plotting ROC Curve
roc = lr_model.summary.roc.toPandas()
plt.plot(roc['FPR'], roc['TPR'])
plt.ylabel('False Positive Rate')
plt.xlabel('True Positive Rate')
plt.title('ROC Curve')
plt.show()


# In[31]:


# print area under curve (of trained data) 
print("Training set areaUnderROC : ")
print(lr_model_summary.areaUnderROC)


# #### Model Evaluation

# In[32]:


predict_train = lr_model.transform(train_data)
predict_test = lr_model.transform(test_data)


# In[40]:


predict_test.columns


# In[42]:


predict_test.select("features", "sex_label", "prediction").show(10)


# In[43]:


predict_test.select("features", "sex_label", "rawPrediction", "prediction", "probability").show(5)


# In[44]:


from pyspark.ml.evaluation import BinaryClassificationEvaluator


# In[45]:


evaluator = BinaryClassificationEvaluator(rawPredictionCol = "rawPrediction", labelCol = "sex_label")


# In[46]:


#check metric used in evaluator
evaluator.getMetricName()


# In[47]:


print("The area under ROC for train set is {}". format(evaluator.evaluate(predict_train)))


# In[48]:


print("The area under ROC for test set is {}". format(evaluator.evaluate(predict_test)))


# Are under ROC of 0.5, conveys that the model has poor class separation capacity. The model must be further improved by hypertuning the parameters of the model.

# In[ ]:




