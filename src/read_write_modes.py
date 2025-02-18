# Databricks notebook source
# DBTITLE 1,imports
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,constants
path = 'dbfs:/FileStore/data_profiling.csv'
schema = "id int, name string, email string, age int, salary int"

# COMMAND ----------

# creating dataframe using permissive mode
df_permissive= spark.read.schema(schema).option('mode', 'permissive').csv(path, header=True)
# creating dataframe using permissive mode
df_dropMalformed= spark.read.schema(schema).option('mode', 'dropmalformed').csv(path, header=True)
# creating dataframe using permissive mode
df_failfast= spark.read.schema(schema).option('mode', 'failfast').csv(path, header=True)

# COMMAND ----------

display(df_permissive)

# COMMAND ----------

df_dropMalformed.display()

# COMMAND ----------

df_failfast.display()

# COMMAND ----------

data = [(1, 'arjun', 100), (1, 'reddy', 200), (1, 'hohan', 300)]
schema = StructType([StructField('id', IntegerType()),StructField('name', StringType()) , StructField('salary', IntegerType())])

# COMMAND ----------

df1 = spark.createDataFrame(data, schema)
df1.display()

# COMMAND ----------

df1.write.mode('append').csv('dbfs:/FileStore/write.csv')

# COMMAND ----------

data2 = [(3, 'arjun', 100), (4, 'reddy', 200), (5, 'gohan', 300)]
df2 = spark.createDataFrame(data2, schema)


# COMMAND ----------

df2.write.mode('append').csv('dbfs:/FileStore/write.csv')

# COMMAND ----------

df3 = spark.read.csv('dbfs:/FileStore/write.csv')

# COMMAND ----------

df3.display()

# COMMAND ----------

df2.write.mode('overwrite').csv('dbfs:/FileStore/write.csv')

# COMMAND ----------

df4 = spark.read.csv('dbfs:/FileStore/write.csv').display()

# COMMAND ----------


