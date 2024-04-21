# Databricks notebook source
#The 'help()' function is used to get the information related to the object or value we passed to the function
help(spark.createDataFrame)

# COMMAND ----------

#Create DataFrame using createDataFrame() by hard coded values 
data = [(1, 'Yaswanth','Wuyyuru'), (2, 'EswaraNadh', 'Kukkala')]
df = spark.createDataFrame(data = data)
df.show()

# COMMAND ----------

#Creating DataFrame using createDataFrame() and also giving schema means giving the header to the columns
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

#Creating DataFrame using createDataFrame by explicitly passing the datatypes
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

#Read List Of CSV Files sing 'spark.read.csv()'
from pyspark.sql.types import *
schema = StructType().add(field = 'MODELYEAR', data_type = IntegerType())\
                     
df = spark.read.csv(['dbfs:/FileStore/pyspark Data1/FuelConsumptionCo2.csv', 'dbfs:/FileStore/pyspark Data/FuelConsumptionCo2.csv'], schema = schema, header = True)

df.show()
df.printSchema()

# COMMAND ----------

#Write DataFrame Into CSV
data = [(1, 'Yaswanth', 'Wuyyuru'), (2, 'Eswarandh', 'Kukkala')]
schema = StructType([StructField(name = 'ID', dataType = IntegerType()),
                     StructField(name = 'FirstName', dataType = StringType()),
                     StructField(name = 'LastName', dataType = StringType())])
df = spark.createDataFrame(data = data, schema = schema)
display(df)
df.printSchema()

# COMMAND ----------

df.write.csv(path = 'dbfs:/FileStore/pyspark Data/FirstWriteSample.csv', header= True)

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/pyspark Data/FirstWriteSample.csv',header = True)
display(df)

# COMMAND ----------

df.write.csv(path = 'dbfs:/FileStore/pyspark Data/FirstWriteSample.csv', header = True, mode = 'ignore')

# COMMAND ----------

df = spark.read.csv('dbfs:/FileStore/pyspark Data/FirstWriteSample.csv', header = True)
display(df)

# COMMAND ----------

#Read JSON Files Using Pyspark
df = spark.read.json('dbfs:/FileStore/pyspark Data/MOCK_DATA.json', multiLine = True)
display(df)
df.printSchema()

# COMMAND ----------

#Write JSON Files Into DataFrame
df.write.json(path = 'dbfs:/FileStore/pyspark Data/FirstWrite.json', mode = 'append')

# COMMAND ----------

#Read The Recently Created Json Write File
schema = StructType().add(field = 'email', data_type = StringType())\
                     .add(field = 'first_name', data_type = StringType())\
                     .add(field = 'gender', data_type = StringType())\
                     .add(field = 'id', data_type = IntegerType())\
                     .add(field = 'ip_address', data_type = StringType())
df = spark.read.json('dbfs:/FileStore/pyspark Data/FirstWrite.json', multiLine = True, schema = schema)
display(df)
df.printSchema()

# COMMAND ----------


