#!/usr/bin/env python
# coding: utf-8

# ## Functions
# 
# 
# 

# In[27]:


import asyncio
import time


# In[28]:


def executor(f):
    def wrapped(*args, **kwargs):
        return asyncio.get_event_loop().run_in_executor(None, f, *args, **kwargs)

    return wrapped


# In[29]:


@executor
def readDelta(path,bu):
    df = spark.read.format("delta").load(path)
    df.createOrReplaceTempView("{}".format(bu))


# In[ ]:


@executor
def readDeltaQuery(query,bu):
    df = spark.sql(query)
    df.createOrReplaceTempView("{}".format(bu))

