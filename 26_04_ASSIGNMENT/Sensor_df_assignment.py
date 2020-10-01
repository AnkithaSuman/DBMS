#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from pyspark.sql import SparkSession


# In[2]:


spark=SparkSession.builder.appName("Occupance_Sensor").getOrCreate()


# In[34]:


df=spark.read.option("header",True).csv("occupancy_sensor_data.txt")


# In[35]:


df.show()


# In[11]:


df.groupby('humidity').avg().show()


# In[13]:


##•	Filter all the timestamps where the temperature is greater than 20 degrees.
df.filter(df.Humidity>20).show()


# In[31]:


df.groupBy('Humidity').max().show()


# In[51]:


##•	What is the maximum and minimum temperature of the room with more than 3 occupants? 
df.filter(df.Occupants>3).agg({'Humidity':'max'}).show()
df.filter(df.Occupants>3).agg({'Humidity':'min'}).show()


# In[36]:


##•	Find out the number of instances where the room has more than 4 occupants.
df.filter(df.Occupants >4).show()


# In[ ]:




