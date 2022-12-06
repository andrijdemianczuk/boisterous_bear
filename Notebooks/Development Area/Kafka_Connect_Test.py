# Databricks notebook source
from pyspark.sql.functions import *

startingOffsets = "earliest" 
kafka_bootstrap_servers_plaintext = "35.86.112.176:9092"
topic = "dev1"

kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", topic )
  .option("startingOffsets", startingOffsets )
  .load())

#read_stream = kafka.select(col("key").cast("string").alias("eventId"), from_json(col("value").cast("string"), input_schema).alias("json"))

read_stream = kafka.select(col("*"))

display(read_stream)
