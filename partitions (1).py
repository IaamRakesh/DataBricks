# Databricks notebook source
sc.defaultParallelism


# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionsBytes")

# COMMAND ----------

from pyspark.sql.types import IntegerType
df = spark.createDataFrame(range(10), IntegerType())

df.rdd.getNumPartitions()


# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", 100000)
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/tables/SalesJan2009.csv")



df1.rdd.getNumPartitions()
df1.rdd.glom().collect()


# COMMAND ----------

df2 = df1.repartition(4)
df2.rdd.getNumPartitions()

df3 = df1.coalesce(1)
df3.rdd.glom().collect()

# COMMAND ----------


