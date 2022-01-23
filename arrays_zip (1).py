# Databricks notebook source
array_data = [
              ("John",4,2),
              ("John",3,4),
               ("David",5,1),
               ("John",6,2),
               ("Mike",9,1),
               ("Mike",4,3),
               ("Mike",7,1),
               ("John",4,3),
                ("John",5,1),
               ("John",7,1),
               ("David",4,1),
              ("David",4,2),
              ("Mike",4,2),
              ("John",8,1),
           ("John",8,1),
           ("David",2,3),
             ]

array_schema = ["Name", "score_1", "score_2"]

arrayDF = spark.createDataFrame(data=array_data, schema=array_schema)

display(arrayDF)

# COMMAND ----------


from pyspark.sql import functions as F
masterDF = arrayDF.groupby("Name").agg(F.collect_list('score_1').alias('Arrays_score_1'),F.collect_list('score_2').alias('Arrays_score_2'))

display(masterDF)


# COMMAND ----------

arrays_zip_DF = masterDF.withColumn("Zipped_Value", F.arrays_zip("Arrays_score_1","Arrays_score_2"))

arrays_zip_DF.show()


# COMMAND ----------

empDF = [
         ('Sales_dept',[{'emp_name':'John','salary':'10000','yrs_of_service':'2','Age':'26'},
                         {'emp_name':'David','salary':'20000','yrs_of_service':'3','Age':'30'},
                        {'emp_name':'Mike','salary':'30000','yrs_of_service':'1','Age':'25'},
                        {'emp_name':'Rakesh','salary':'50000','yrs_of_service':'4','Age':'27'},
                        {'emp_name':'Bhai','salary':'60000','yrs_of_service':'5','Age':'25'}]),
        ('HR_dept',[{'emp_name':'John','salary':'10000','yrs_of_service':'2','Age':'26'},
                         {'emp_name':'David','salary':'20000','yrs_of_service':'3','Age':'30'},
                        {'emp_name':'Mike','salary':'30000','yrs_of_service':'1','Age':'25'},
                        {'emp_name':'Rakesh','salary':'50000','yrs_of_service':'4','Age':'27'},
                        {'emp_name':'Bhai','salary':'60000','yrs_of_service':'5','Age':'25'}])
    
       ]

df_brand = spark.createDataFrame(data=empDF, schema = ['Department','Employee'])
df_brand.printSchema()

display(empDF)

# COMMAND ----------

df_brandZip = df_brand.withColumn("Zip",F.arrays_zip(df_brand["Employee"]))

display(df_brandZip)

# COMMAND ----------

df_brand_exp = df_brandZip.withColumn("Explode",F.explode(df_brandZip.Zip))

display(df_brand_exp)

# COMMAND ----------

df_brand_output = df_brand_exp.withColumn("employee_emp_name",df_brand_exp['Explode.Employee.emp_name'])\
                              .withColumn("employee_yrs_of_service",df_brand_exp['Explode.Employee.yrs_of_service'])\
                              .withColumn("employee_salary",df_brand_exp['Explode.Employee.salary'])\
                              .withColumn("employee_Age",df_brand_exp['Explode.Employee.Age']).drop("Explode").drop("Zip").drop("Employee")

# COMMAND ----------

display(df_brand_output)

# COMMAND ----------


