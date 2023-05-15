# Databricks notebook source
from pyspark.sql.functions import *
from datetime import datetime
from time import time
from pyspark.sql.window import *

# COMMAND ----------

#Get everything since this morning at midnight
df = spark.table("field_demos.canwest_sa.ad_dts2_melted_silver").filter(col('timestamp') >= date_sub(current_date(), 0))

# COMMAND ----------

display(df)

# COMMAND ----------

#This is kind of our rolling daily average that gets updated throughout the day.
df = df.groupBy("well", "segment").agg(avg("temp").alias("avg_temp"))

# COMMAND ----------

#bring the aggregates back to the driver node and cache and commit
df.collect()
df.cache()
df.count()

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format('delta').option("mergeSchema", "true").partitionBy("well").mode('overwrite').saveAsTable("field_demos.canwest_sa.ad_dts2_daily_temps")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Window (avg, mean, med, max & min) by depth (segment) over time

# COMMAND ----------

df = spark.table("ademianczuk.ad_dlt_dts2_melted").filter(col('timestamp') >= date_sub(current_date(), 0))

# COMMAND ----------

df = df.withColumn("minute", minute("timestamp")).withColumn("hour", hour("timestamp")).withColumn("day", dayofyear("timestamp")).withColumn("month", month("timestamp"))

# COMMAND ----------

df.show(5)

# COMMAND ----------

windowSpec = Window.partitionBy("segment", "well", "hour").orderBy("day")

df.withColumn("avg", avg(col("temp")).over(windowSpec)).withColumn("min", min(col("temp")).over(windowSpec)).withColumn("max", max(col("temp")).over(windowSpec)).withColumn("stddev", stddev(col("temp")).over(windowSpec)).withColumn("cume_dist",cume_dist().over(windowSpec)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Time-based Windows

# COMMAND ----------

df = spark.table("ademianczuk.ad_dlt_dts2_melted")

# COMMAND ----------

windowedDF = df.groupBy("segment", window("timestamp", "10 minutes", "5 minutes")).count()

# COMMAND ----------

display(df.groupBy("well", "segment", window("timestamp", "10 minutes", "5 minutes")) 
        .agg(stddev("temp").alias("stddev_temp"), avg("temp").alias("avg_temp"), min("temp").alias("min_temp"), max("temp").alias("max_temp")) \
        .orderBy(col("window.start")))
