# Databricks notebook source
type(spark)

# COMMAND ----------

help(spark)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

from pyspark.sql.types import *

data = [(1, 'Yaswanth', 'Wuyyuru')]
schema = ['ID', 'FirstName', 'LastName']
df = spark.createDataFrame(data = data, schema = schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
data = [(1, 'Yaswanth', 'Wuyyuru'), (2, 'Eswaranadh','Kukkala')]
schema = StructType([StructField(name = 'ID', dataType = IntegerType()),
                     StructField(name = 'FirstName', dataType = StringType()),
                     StructField(name = 'LastName', dataType = StringType())])
df = spark.createDataFrame(data = data, schema = schema)
df.show()
df.printSchema()

# COMMAND ----------

#Read Data From CSV Files Using Pyspark

from pyspark.sql.types import *

df = spark.read.csv('dbfs:/FileStore/pyspark Data1/EmployeeCityData__2_.csv', header = True)
df.show()
df.printSchema()

# COMMAND ----------

#Explicitly give the data types for the column headers

from pyspark.sql.types import *

schema = StructType()\
                    .add(field = 'id', data_type = IntegerType())\
                    .add(field = 'first_name', data_type = StringType())\
                    .add(field = 'last_name', data_type = StringType())\
                    .add(field = 'email', data_type = StringType())\
                    .add(field = 'gender', data_type = StringType())

df = spark.read.csv('dbfs:/FileStore/pyspark Data1/EmployeeCityData__2_.csv', header = True, schema = schema)
df.show()
df.printSchema()

# COMMAND ----------


