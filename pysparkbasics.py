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
from pyspark.sql.types import *
schema = StructType([StructField(name = 'email', dataType = StringType()),
                     StructField(name = 'first_name', dataType = StringType()),
                     StructField(name = 'gender', dataType = StringType()),
                     StructField(name = 'id', dataType = IntegerType()),
                     StructField(name = 'ip_address', dataType = StringType()),
                     StructField(name = 'last_name', dataType = StringType())])
df = spark.read.json('dbfs:/FileStore/pyspark Data/MOCK_DATA.json', multiLine = True, schema = schema)
df.show()
df.printSchema()

# COMMAND ----------

#Write JSON Files Into DataFrame
from pyspark.sql.types import *
schema = StructType().add(field = 'email', data_type = StringType())\
                     .add(field = 'first_name', data_type = StringType())\
                     .add(field = 'gender', data_type = StringType())\
                     .add(field = 'id', data_type = IntegerType())\
                     .add(field = 'ip_address', data_type = StringType())
df.write.json(path = 'dbfs:/FileStore/pyspark Data/FirstWrite.json', mode = 'append')
df.printSchema()

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

#Read Parquet Files Into DataFrame

from pyspark.sql.types import *
df = spark.read.parquet('dbfs:/FileStore/pyspark Data/parquetfilesdata/*.parquet')
display(df)
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([StructField(name = 'registration_dttm', dataType = TimestampType()),
                     StructField(name = 'id', dataType = LongType()),
                     StructField(name = 'first_name', dataType = StringType()),
                     StructField(name = 'last_name', dataType = StringType()),
                     StructField(name = 'email', dataType = StringType()),
                     StructField(name = 'gender', dataType = StringType()),
                     StructField(name = 'ip_address', dataType = StringType()),
                     StructField(name = 'cc', dataType = StringType()),
                     StructField(name = 'country', dataType = StringType()),
                     StructField(name = 'birthdate', dataType =StringType()),
                     StructField(name = 'salary', dataType = DoubleType()),
                     StructField(name = 'title', dataType = StringType()),
                     StructField(name = 'comments', dataType = StringType())])
df = spark.read.parquet('dbfs:/FileStore/pyspark Data/parquetfilesdata/*.parquet', schema = schema)
display(df)
df.printSchema()

# COMMAND ----------

df.write.parquet(path = 'dbfs:/FileStore/pyspark Data/parquetfilesdata/parquetwriteoperations/firstparquetwrite.parquet', mode = 'append')
#df.write.parquet(path = 'dbfs:/FileStore/pyspark Data/parquetfilesdata/parquetwriteoperations/firstparquetwrite.parquet', mode = 'overwrite')
#df.write.parquet(path = 'dbfs:/FileStore/pyspark Data/parquetfilesdata/parquetwriteoperations/firstparquetwrite.parquet', mode = 'ignore')
#df.write.parquet(path = 'dbfs:/FileStore/pyspark Data/parquetfilesdata/parquetwriteoperations/firstparquetwrite.parquet', mode = 'error')
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *

df = spark.read.parquet('dbfs:/FileStore/pyspark Data/parquetfilesdata/parquetwriteoperations/firstparquetwrite.parquet')
display(df)
print('Count Of Records In The DataFrame is: ' + str(df.count()) + ' ' + 'Records')
df.printSchema()

# COMMAND ----------

#Create Tuples Of Data And Write It Into A DataFrame

from pyspark.sql.types import *

data = [(1, 'Yaswanth', 'Wuyyuru', 'yaswanth@reminr.com'),
        (2, 'Eswaranadh', 'Kukkala', 'eswaranadh@reminr.com'),
        (3, 'Kalyan Babu', 'Tangella', 'kalyanbabu@reminr.com')]
schema = StructType([StructField(name = 'ID', dataType = IntegerType()),
                    StructField(name = 'FirstName', dataType =StringType()),
                    StructField(name = 'LastName', dataType = StringType()),
                    StructField(name = 'Email', dataType = StringType())])
df = spark.createDataFrame(data = data, schema = schema)
display(df)
df.write.parquet(path = 'dbfs:/FileStore/pyspark Data/parquetfilesdata/parquetwriteoperations/Secondparquetwrite.parquet', mode = 'overwrite')
df.printSchema()
print('Total Number Of Records: ' + str(df.count()) +  ' ' 'Records')


# COMMAND ----------

#Show() To Display DataFrame Contents

data = [(1, 'Yaswanth', 'Wuyyuru', 'yaswanth@reminr.com'),
        (2, 'Eswaranadh', 'Kukkala', 'eswaranadh@reminr.com'),
        (3, 'Kalyan Babu', 'Tangella', 'kalyan@reminr.com')]
schema = StructType([StructField(name = 'ID', dataType = IntegerType()),
                     StructField(name = 'FirstName', dataType = StringType()),
                     StructField(name = 'LastName', dataType = StringType()),
                     StructField(name = 'Email', dataType = StringType())])
df = spark.createDataFrame(data = data, schema =schema)
df.show()

# COMMAND ----------

from pyspark.sql.types import *
df = spark.read.csv('dbfs:/FileStore/pyspark Data/FuelConsumptionCo2.csv', header = True)
df.show(truncate = False)

# COMMAND ----------

#withColumn()--> It Is A Trnasformation Function Whcih Is Used To Create A New Column Or Change The Value In A Column Or Change The Column DataType and Many More
from pyspark.sql.types import *
data = [(1, 'Yaswanth', 'Wuyyuru', 'yaswanth@reminr.com', 'Dallas', 'Texas', '70000000'),
        (2, 'Eswaranadh', 'Kukkala', 'eswaranadh@reminr.com', 'Florida', 'Atlanta','70000000'),
        (3, 'Kalyan Babu', 'Tangella', 'kalyan@reminr.com', 'Florida', 'Atlanta','70000000')]

schema = StructType([StructField(name = 'ID', dataType = IntegerType()),
                     StructField(name = 'FirstName', dataType = StringType()),
                     StructField(name = 'LastName', dataType = StringType()),
                     StructField(name = 'Email', dataType = StringType()),
                     StructField(name = 'City', dataType = StringType()),
                     StructField(name = 'State', dataType = StringType()),
                     StructField(name = 'Salary', dataType = StringType())])
df = spark.createDataFrame(data = data, schema = schema)
df.show(truncate = False)

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

#Update Salary Column Values With withColumn() function 

from pyspark.sql.types import *
df.withColumn('UpdatedSalary', df.Salary + 200000).show(truncate = False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
df1 = df.withColumn(colName = 'Salary', col = col('Salary').cast('Integer'))
df1.show()
df1.printSchema()
df.show()
df.printSchema()

# COMMAND ----------

#Changing DataType Of The Column Without Using Col() Function
from pyspark.sql.types import *
df2 = df.withColumn('Salary', df.Salary.cast('Integer'))
df2.show()
df2.printSchema()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

df3 = df.withColumn(colName = 'UpdatedSalary', col = col('Salary') + 20000000)
display(df3)
df3.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import lit

df1 = df.withColumn('Country', lit('United States Of America'))
df1.display()

# COMMAND ----------

from pyspark.sql.functions import col
df2 = df.withColumn('CopiedSalary', col('Salary'))
display(df2)
