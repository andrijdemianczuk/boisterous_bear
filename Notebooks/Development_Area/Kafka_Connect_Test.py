# Databricks notebook source
from pyspark.sql.functions import *

startingOffsets = "latest" 
kafka_bootstrap_servers_plaintext = "35.86.112.176:9092"
topic = "dts-test1"

kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", topic )
  .option("startingOffsets", startingOffsets )
  .load())

#read_stream = kafka.select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), input_schema).alias("json"))

read_stream = kafka.select(col("*"))

display(read_stream)

# COMMAND ----------

from pyspark.sql.functions import col
df = kafka.select(col("value").cast("string").alias("plaintextValue"))

# COMMAND ----------

display(df)
