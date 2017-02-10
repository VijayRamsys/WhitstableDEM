
# coding: utf-8

# In[83]:

from __future__ import print_function

from pyspark.sql import SparkSession

from pyspark.sql import Row

from pyspark.sql.types import *

import matplotlib.pyplot as plt

import pandas as pd  

import numpy as np

import pylab as P

get_ipython().magic(u'matplotlib inline')

plt.rcdefaults()

incidentDAT = sc.textFile("/whitstable/data/incident.txt")
incidentDAT.count()


# In[84]:

incidentRDD = incidentDAT.map(lambda x: x.split("\t")).map(lambda x: Row(x[0], x[1], x[2], x[3], x[4], x[5], x[6] ))
incidentDFrame = incidentRDD.toDF(["gross_ton","vlength","vdepth","vessel_class","vessel_age","route_type","Accident"])
incidentDFrame.printSchema()
incidentDFrame.show(5)
incidentDFrame.createOrReplaceTempView("vessel")


# # Distribution of vessel class

# In[85]:

VesselClassDF = spark.sql("SELECT vessel_class, count(vessel_class) as vessel_classcount FROM vessel GROUP BY vessel_class ORDER BY vessel_classcount")
VesselClassDF.show()


# In[86]:

vessel_classTuple = VesselClassDF.rdd.map(lambda p: (p.vessel_class,p.vessel_classcount)).collect()
vessel_classList, vessel_classcountList = zip(*vessel_classTuple)


# In[87]:

explode = (0.1,0.2,0.4,0.6,0.8,0.9,0.2,0.4,0.6,0.8,0.6,0.4,0.2,0.1,0.1,0.2)  
plt.pie(vessel_classcountList, explode=explode, labels=vessel_classList, autopct='%1.1f%%', shadow=True, startangle=45)
plt.title('\n')
plt.show(block=False)


# #### Distribution of vessel class involved in accidents

# In[88]:

vesselAccident = spark.sql("SELECT vessel_class, Accident FROM vessel")
vesselAccident.show()


# In[89]:

vesselCrossTab = vesselAccident.stat.crosstab("vessel_class", "Accident")
vesselCrossTab.show()


# In[90]:

vesselCrossTab.describe('No', 'Yes').show()


# In[91]:

vesselCrossTab.stat.cov('No', 'Yes')


# In[73]:

vesselCrossTab.stat.corr('No', 'Yes')


# In[92]:

vsaTuple = vesselCrossTab.rdd.map(lambda p: (p.vessel_class_Accident,p.No, p.Yes)).collect()
vsaList, nList, yList = zip(*vsaTuple)


# In[109]:

N = len(vsaList)
ind = np.arange(N)    
width = 0.75          
p1 = plt.bar(ind, nList, width, color='b')
p2 = plt.bar(ind, yList, width, color='r', bottom=nList)
plt.ylabel('Count')
plt.title('distribution of vessel class involved in accidents\n')
plt.xticks(ind + width/2., vsaList, rotation=90)
plt.legend((p1[0], p2[0]), ('No', 'Yes'))
plt.gcf().subplots_adjust(bottom=0.10)
plt.show(block=False)


# In[ ]:



