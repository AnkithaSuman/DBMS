#!/usr/bin/env python
# coding: utf-8

# In[10]:


import findspark
findspark.init()
from pyspark.sql import SparkSession


# In[2]:


spark=SparkSession.builder.appName("Airlines").getOrCreate()


# In[3]:


spark


# In[4]:


df=spark.read.option("header",True).csv("flights.csv")


# In[5]:


df.show()


# In[8]:


df.printSchema()


# In[12]:


df.columns


# In[10]:


df


# In[15]:


type(df['date'])


# In[18]:


##Select the airlines with their origin and destination
df.select('airlines','origin','destination').show()


# In[21]:


##Select flights with origin 'IND' 
df.filter(df.origin =='IND').show()


# In[ ]:


df.select('airlines','flight_number','origin').filter(df.origin =='IND').show()


# In[22]:


##•	Select flights with destination as 'DFW.
df.filter(df.destination =='DFW').show()


# In[20]:


df.show()


# In[22]:


df.select('airlines','air_time').withColumn('duration_hrs',df.air_time/60).show()


# In[60]:


df.count()


# In[6]:


##•	Select flights with origin as 'SEA' and 'DFW'
df.select('airlines','flight_number','origin',).filter((df.origin =='SEA') | (df.origin == 'DFW')).show()


# In[7]:


##List the flights having air_time more than 120 mins.
df.filter(df.air_time > 120).show()


# In[12]:


df.count()


# In[6]:


from pyspark.sql.types import StructField, IntegerType,StructType,StringType


# In[12]:


flights_schema=[StructField("date",IntegerType(),True),
            StructField("airlines",IntegerType(),True),
            StructField("flight_number",IntegerType(),True), 
            StructField("origin",IntegerType(),True), 
            StructField("destination",IntegerType(),True), 
            StructField("departure",IntegerType(),True),  
            StructField("departure_delay",IntegerType(),True),  
            StructField("arrival",IntegerType(),True),  
            StructField("arrival_delay",IntegerType(),True),  
            StructField("air_time",IntegerType(),True),  
            StructField("distance",IntegerType(),True)]


# In[13]:


final_struc= StructType(fields=flights_schema)


# In[14]:


flights_df=spark.read.csv("flights.csv",schema=final_struc)


# In[15]:


flights_df.printSchema()


# In[ ]:





# In[81]:


##Add a new columns to store the duration in hrs
df.select('airlines','air_time').withColumn('duration_hrs',df.air_time/60).show()


# In[80]:


##List flights covering distance more than 1500
df.select('airlines','origin','destination','distance').filter(df.distance> 1500).show()


# In[22]:


##Find the flight with min airtime
df.select('airlines','air_time').agg({'air_time':'min'}).show()


# In[25]:



df.select('airlines','air_time').agg({'air_time':'max'}).show()


# In[32]:


##Find the flight with max airtime
df.groupBy('airlines').agg({'air_time':'max'}).show()


# In[35]:


##Find the flight with min duration_hrs
df.select('airlines','air_time').withColumn('duration_hrs',df.air_time/60).agg({'duration_hrs':'min'}).show()


# In[37]:


##•	Find the flight with max duration_hrs
df.select('airlines','air_time').withColumn('duration_hrs',df.air_time/60).groupby().max('duration_hrs').show()


# In[48]:


##•	Find the flight with max distance covered
df.groupby('flight_number').agg({'distance':'max'}).show()


# In[50]:


##•	Find the flight with min distance covered
df.agg({'distance':'min'}).show()


# In[54]:


df.groupby(['airlines']).agg({'air_time':'avg'}).show()


# In[79]:


##•	Find the average duration of the flights.
df.groupby(['airlines']).agg({'air_time':'avg'}).show()


# In[61]:


df1=spark.read.csv('flights.csv', inferSchema=True,header=True)


# In[62]:


##•	Find the shortest flight from 'PDX' with respect to distance.
df1.filter(df1.origin=='PDX').groupby('origin').min('distance').show()


# In[72]:


##•	Find the above for the flight 'OGG', 'BOS', 'JFK'
df1.filter((df1.origin=='OGG') | (df1.origin=='BOS') | (df1.origin=='JFK')).groupby('origin').min('distance').show()


# In[74]:


##•	Find the longest flight from 'SEA' with respect to duration
df1.filter(df1.origin=='SEA').groupby('origin').max('air_time').show()


# In[75]:


##•	find the above for the flight 'ORD','MIA', 'HNL'
df1.filter((df1.origin=='ORD') | (df1.origin=='MIA') | (df1.origin=='HNL')).groupby('origin').max('air_time').show()


# In[76]:


##•	Find the average departure delay for the airline no 20366, 20398, 19805
df1.filter((df1.airlines==20366) | (df1.airlines==20398) | (df1.airlines==19805)).groupby('airlines').avg('departure_delay').show()


# In[77]:


##•	similarly, find the average arrival delay for all the airline nos.
df1.groupby('airlines').avg('arrival_delay').show()


# In[ ]:




