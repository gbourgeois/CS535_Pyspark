#!/usr/bin/env python
# coding: utf-8

# # Case Analysis or Capitalization Probability

# In[1]:


get_ipython().system('pip install pyspark')


# In[2]:


import os


# In[3]:


from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Homework').setMaster('local[*]')
sc = SparkContext.getOrCreate(conf=conf)


# In[4]:


import itertools
#path = "/Users/bourgeoisgadjagb/Documents/CS535_Spring2022/file123"
path = "/Users/bourgeoisgadjagb/Documents/CS535_Spring2022/test_data"
rdd = sc.wholeTextFiles(path)


# In[5]:


rdd2=rdd.flatMap(lambda data: data[1].split())
rdd2.collect()


# In[6]:


rdd3=rdd2.flatMap(lambda x: [(y, 1) if y.isupper()==True else (y.upper(), 0) for y in x] )
rdd3.collect()


# In[7]:


rdd4 = rdd3.groupByKey().mapValues(lambda x: sum(x)/len(x)*100)
rdd4.collect()


# In[ ]:




