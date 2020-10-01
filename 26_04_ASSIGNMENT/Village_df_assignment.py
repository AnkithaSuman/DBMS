#!/usr/bin/env python
# coding: utf-8

# In[21]:


import findspark
findspark.init()
from pyspark.sql import SparkSession


# In[2]:


spark=SparkSession.builder.appName("Village1").getOrCreate()


# In[31]:


df1=spark.read.option("header",True).csv("DCHB_Village_Amenities_Bangalore.csv")


# In[6]:


df1.printSchema()


# In[32]:


df=spark.read.csv('DCHB_Village_Amenities_Bangalore.csv', inferSchema=True,header=True)


# In[33]:


df.printSchema()


# In[20]:


df.select('Village Code','Village Name','Total Population of Village','Nearest Town Name','Nearest_Town_Distance_Village').filter(df.Nearest_Town_Distance_Village<= 10).show()


# In[30]:


##•	Find out Bthe total number of villages which are having primary health centres (PHCs).
df.filter(df.Primary_Health_Centre_Numbers==1).count()


# In[ ]:


# •	Find out the number of villages which cultivate paddy


# In[36]:


df.filter(df.Agricultural_Commodities_First=='PADDY').count()


# In[37]:


df.filter(df.Agricultural_Commodities_Second=='PADDY').count()


# In[38]:


df.filter(df.Agricultural_Commodities_Third=='PADDY').count()


# In[34]:


df.filter((df.Agricultural_Commodities_First=='PADDY') | (df.Agricultural_Commodities_Second=='PADDY') | (df.Agricultural_Commodities_Third=='PADDY')).count()


# In[ ]:




