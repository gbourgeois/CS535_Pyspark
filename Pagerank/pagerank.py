#!/usr/bin/env python
# coding: utf-8

# In[2]:


#!pip install pyspark
import os
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Homework').setMaster('local[*]')
sc = SparkContext.getOrCreate(conf=conf)


# In[98]:


import itertools
path = "/Users/bourgeoisgadjagb/Documents/CS535_Spring2022/Pagerank/pagerank_data_5.txt"
rdd = sc.wholeTextFiles(path)
rdd.collect()


# In[99]:


rdd2 = rdd.flatMap(lambda x: [d.split() for d in x[1].split('\n') if d!=''])
#rdd2 = rdd2
#.distinct()
rdd3 = rdd2.groupByKey().mapValues(list)
rdd3.collect()


# In[100]:


rdd4 = rdd3.map(lambda x: (x[0], 1.0))
rdd4.collect()


# In[101]:


max_iter = 12
for i in range(5):
    update_cont = rdd3.join(rdd4).flatMap(lambda x:[(b, x[1][1]/len(x[1][0])) for b in x[1][0]])    .groupByKey().mapValues(sum)
    rdd4 = update_cont


# In[102]:


rdd4.collect()

