# Databricks notebook source
data = [(1, 'arjun'), (2, 'alice'), (3, 'bob')]
schema = ['id','name'] 
df = spark.createDataFrame(data, schema)
df.display()

# COMMAND ----------

data = ((1, 'arjun', 100), (2, 'alice', 100), (3, 'bob', 100))
schema = ['id','name', 'score'] 
df1= spark.createDataFrame(data, schema)

# COMMAND ----------

#df.write.mode('overwrite').format('delta').save('/Users/arjun37ca@gmail.com/delta')
df.write.format('delta').mode('overwrite').save('/Users/arjun37ca@gmail.com/delta')


# COMMAND ----------

rd = spark.read.format('delta').load('/Users/arjun37ca@gmail.com/delta')
rd.display()

# COMMAND ----------

df1.write.mode('append').format('delta').option('mergeSchema', True).save('/Users/arjun37ca@gmail.com/delta')

# COMMAND ----------

rd1 = spark.read.format('delta').load('/Users/arjun37ca@gmail.com/delta')
rd1.display()

# COMMAND ----------

df.write.mode('overwrite').format('delta').saveAsTable('dt1')

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into dt1 values(4, 'max');

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE dt1 TO VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dt1 version as of 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC update dt1 set name='boxer' where id=1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize dt1;

# COMMAND ----------

df = spark.read.table('dt1')
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history dt1;

# COMMAND ----------

df3 = spark.read.format('delta').option('versionAsOf', 0).load('dbfs:/Users/arjun37ca@gmail.com/delta')
df3.display()

# COMMAND ----------

df4 = spark.read.format('delta').option('versionAsOf', 1).load('dbfs:/Users/arjun37ca@gmail.com/delta')
df4.display()
