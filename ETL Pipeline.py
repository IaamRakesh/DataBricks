# Databricks notebook source
connectionString = "jdbc:sqlserver://rakesh9377.database.windows.net:1433;database=rakesh;user=rakesh@rakesh9377;password={apc@12345};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

df_product = spark.read.jdbc(connectionString,'SalesLT.PRODUCT')

display(df_product)

# COMMAND ----------

df_sales = spark.read.jdbc(connectionString,"SalesLT.SalesOrderDetail")

display(df_sales)

# COMMAND ----------

df_product_clen = df_product.na.fill({"Size": "M", "Weight": "100"})

display(df_product_clen)

# COMMAND ----------

df_sales_clen = df_sales.dropDuplicates()
display(df_sales_clen)

# COMMAND ----------

df_join = df_sales_clen.join(df_product_clen,df_sales_clen.ProductID==df_product_clen.ProductID,"leftouter").select(df_sales_clen.ProductID,
                                                      df_sales_clen.UnitPrice,
                                                      df_sales_clen.LineTotal,
                                                      df_product_clen.Name,
                                                      df_product_clen.Color,
                                                      df_product_clen.Size,
                                                      df_product_clen.Weight)
display(df_join)

# COMMAND ----------

df_agg = df_join.groupBy(["ProductID","Name","Color","Size","Weight"]).sum("LineTotal").withColumnRenamed("sum(LineTotal)","sum_total_sales")

display(df_agg)

# COMMAND ----------

dbutils.fs.mount(
         source = "wasbs://rakesh@rockydatalake.blob.core.windows.net/",
         mount_point = "/mnt/rak",
         extra_configs = {"fs.azure.account.key.rockydatalake.blob.core.windows.net":"+UE6Ew6VihrBI0HxqPdX82RpjVQq4o/lTWR1N5b7soC/v557WD3TU5C/+OgV4aAIClzebVW3Mi2bk9EwjX9ptw=="}
          )

# COMMAND ----------

dbutils.fs.ls("/mnt/rak")

# COMMAND ----------

df_agg.write.format("parquet").save("mnt/rak/rak2_parquet/")

# COMMAND ----------

df_agg.write.format("csv").save("mnt/rak/rak3_csv/")

# COMMAND ----------


