#!/usr/bin/env python
# coding: utf-8

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
path = "/Users/bourgeoisgadjagb/Documents/CS535_Spring2022/dataN.txt"
rdd = sc.wholeTextFiles(path)


# In[5]:


rdd.collect()


# #Find the number of citations for each patent in a patent reference data set. The format of the input is
# citing_patent, cited_patent

# In[6]:


rdd1 = rdd.flatMap(lambda file: [tuple(reversed(d.split())) for d in file[1].split('\n') if d!=''])
rdd2 = rdd1.groupByKey().mapValues(lambda vals:len(vals))
rdd2.collect()


# #Find the top N most frequently cited patents. The format of the input is
# citing_patent, cited_patent

# In[7]:


rdd2.sortBy(lambda x: x[1], ascending = False).collect()

