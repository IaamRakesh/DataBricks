# Databricks notebook source
dbutils.fs.mkdirs("/FileStore/tables/stream_checkpoint/")
dbutils.fs.mkdirs("/FileStore/tables/stream_read/")
dbutils.fs.mkdirs("/FileStore/tables/stream_write/")


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema_defined = StructType([
                              StructField('File', StringType(), True),
                              StructField('shop', StringType(), True),
                              StructField('Sales_count',IntegerType(), True)
                            ])



# COMMAND ----------

df = spark.readStream.format("csv").schema(schema_defined).option("header",True).option("sep",";").load("/FileStore/tables/stream_read")

display(df)

# COMMAND ----------

df2 = df.writeStream.format("csv").outputMode("append").option("path","/FileStore/tables/stream_write/").option("checkpointlocation","/FileStore/tables/stream_checkpoint/").start().awaitTermination()
display(df2)
