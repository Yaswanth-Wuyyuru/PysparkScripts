# Databricks notebook source
help(spark.createDataFrame)

# COMMAND ----------

data = [(1, 'Yaswanth','Wuyyuru'), (2, 'EswaraNadh', 'Kukkala')]
df = spark.createDataFrame(data = data)
df.show()

# COMMAND ----------

data = [(1, 'Bhaskara Rao', 'Wuyyuru'), 
        (2, 'Ramani','Wuyyuru'), 
        (3, 'Yaswanth Surya', 'Wuyyuru'), 
        (4, 'Subhashini', 'Wuyyuru'), 
        (5, 'Tanuja', 'Wuyyuru')]
schema = ['id', 'FirstName', 'LastName']
dataframe = spark.createDataFrame(data = data, schema = schema)
dataframe.show()
dataframe.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
data = [(1, 'Yaswanth'),
        (2, 'Eswaranadh')]
schema = StructType([StructField(name = 'ID', dataType = IntegerType()),
                      StructField(name = 'FirstName', dataType = StringType())])
df = spark.createDataFrame(data = data, schema = schema)
df.show()
df.printSchema()

# COMMAND ----------

Data = [(1, 'Yaswanth', 'Wuyyuru'),
        (2, 'Eswaranadh', 'Kukkala')]
schema = StructType([StructField(name = 'ID', dataType = IntegerType()),
                     StructField(name = 'FirstName', dataType = StringType()),
                     StructField(name = 'LastName', dataType = StringType())])
df = spark.createDataFrame(data = Data, schema = schema)
df.show()  
df.printSchema()

# COMMAND ----------

#Read CSV Files Into DataFrame Using Pyspark

from pyspark.sql.types import *

df = spark.read.csv('dbfs:/FileStore/pyspark Data/FuelConsumptionCo2.csv', header = True)
df.show()
 

# COMMAND ----------

from pyspark.sql.types import *
schema = StructType().add(field = 'MODELYEAR', data_type = IntegerType())\
                     .add(field = '')
df = spark.read.csv(['dbfs:/FileStore/pyspark Data1/FuelConsumptionCo2.csv', 'dbfs:/FileStore/pyspark Data/FuelConsumptionCo2.csv'], schema = schema, header = True)

df.show()
df.printSchema()

# COMMAND ----------


