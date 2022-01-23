# Databricks notebook source
empDF = [
         ('David',[4,2,3,7],[5,6,7,4]),
         ('Mike',[9,1,3,4,5],[0,1,9,2,7,6]),
         ('Rakesh',[8,3,2],[1,9,2])
    
]

df = spark.createDataFrame(data=empDF, schema=["Names","arrays_1","arrays_2"])

df.show()

# COMMAND ----------

from pyspark.sql import functions as F

arrays_intersect = df.withColumn("Intersect_Values",F.array_intersect(df["arrays_1"],df["arrays_2"]))

arrays_intersect.show()

# COMMAND ----------

 arrays_except = df.withColumn("Intersect",F.array_except(df["arrays_1"],df["arrays_2"]))
 arrays_except.show()

# COMMAND ----------

 
