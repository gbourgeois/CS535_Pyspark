#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install pyspark')


# In[2]:


import os


# In[5]:


from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Homework').setMaster('local[*]')
sc = SparkContext.getOrCreate(conf=conf)


# In[6]:


import itertools
path = "/Users/bourgeoisgadjagb/Documents/CS535_Spring2022/test_data"
rdd = sc.wholeTextFiles(path)


# In[33]:


output=rdd.map(lambda File: [(word, 1) for word in File[1].split()])


# In[34]:


flat = output.flatMap(lambda x: x)


# In[35]:


sorted(flat.groupByKey().mapValues(len).collect())


# In[ ]:




