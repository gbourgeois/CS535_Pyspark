#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Homework').setMaster('local[*]')
sc = SparkContext.getOrCreate(conf=conf)


# In[2]:


import os


# In[3]:


import itertools
path = "/Users/bourgeoisgadjagb/Documents/CS535_Spring2022/Cross-Correlation/data_2.txt"
rdd = sc.wholeTextFiles(path)


# In[4]:


from itertools import combinations


# In[5]:


rdd1 = rdd.flatMap(lambda file: [d.split() for d in file[1].split('\n') if d!=''])
#rdd2 = rdd1.map(lambda file: (x,y) for x,y in file )
rdd1.collect()
rdd.take(1)


# In[12]:


rdd2 = rdd1.map(lambda x: list(combinations(x, 2))).flatMap(lambda x: x)
rdd3 = rdd2.map(lambda x: (x,1)).groupByKey().mapValues(lambda vals:len(vals))
rdd3.sortBy(lambda x: x[1], ascending = False)
rdd3.collect()


# In[ ]:




