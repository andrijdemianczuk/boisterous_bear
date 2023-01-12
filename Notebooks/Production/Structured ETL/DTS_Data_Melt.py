# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

import sys
import os

# COMMAND ----------

# DBTITLE 1,Read the table into a dataframe
df = spark.table("field_demos.canwest_sa.ad_dts2_json")
df = df.select("timestamp", "well", "coordinates", df.colRegex("`^seg.*`")).withColumn("timestamp", col("timestamp").cast("TimeStamp")).orderBy(col("timestamp").desc())

# COMMAND ----------

# DBTITLE 1,Truncate the dataframe to a 24-hour period
df = df.filter(col('timestamp') <= date_sub(current_date(), 0)).filter(col('timestamp') >= date_sub(current_date(), 1))

# COMMAND ----------

# DBTITLE 1,Un-table the dataset to reduce dimensionality
pivotDF = df.melt(ids=["timestamp","well", "coordinates"], values=df.colRegex("`^seg.*`"), variableColumnName="segment", valueColumnName="temp")
pivotDF.coalesce(8) #Reduces the number of partitions in a dataframe

# COMMAND ----------

# DBTITLE 1,Create a GUID for UPSERTs based on the combination of three columns
pivotDF = pivotDF.withColumn("sha2", sha2(concat_ws("", col("timestamp"), col("well"), col("segment")), 256))

# COMMAND ----------

# DBTITLE 1,Write to Delta
pivotDF.write.format('delta').option("mergeSchema", "true").partitionBy("well").mode('overwrite').saveAsTable("field_demos.canwest_sa.ad_dts2_melted_silver")

# COMMAND ----------

# DBTITLE 1,Optimize
from delta.tables import *
deltaTable = DeltaTable.forName(spark, "field_demos.canwest_sa.ad_dts2_melted_silver")
deltaTable.optimize().executeCompaction()
