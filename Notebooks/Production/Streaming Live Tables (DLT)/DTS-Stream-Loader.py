# Databricks notebook source
# MAGIC %md
# MAGIC # SAGD DTS Sensor Demo
# MAGIC <!-- img src="https://github.com/andrijdemianczuk/boisterous_bear/blob/098be814f5cc945d2ab3e53958679593411f98eb/Notebooks/Production/Streaming%20Live%20Tables%20(DLT)/web.glasfaserkabel.jpeg" / -->
# MAGIC <img src="/files/Users/andrij.demianczuk@databricks.com/resources/web_glasfaserkabel.jpeg" />

# COMMAND ----------

# MAGIC %md
# MAGIC ## Disclaimer
# MAGIC 
# MAGIC This project is for demonstration purposes only. All data is artificially generated and a gross simulated representation of a data feed. No actual field data is used in this demonstration.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Input Definition
# MAGIC 
# MAGIC Here we will use the DLT UI to help us define the input parameters:
# MAGIC * Bootstrap Servers: The collection of Kafka brokers that we will be listening to for events
# MAGIC * Topic: The specific topic we are interested in. This is defined on the Kafka cluster by Zookeeper
# MAGIC * Schema Location: A sample file containing a single event that we will be using to get the schema of our event payload

# COMMAND ----------

kafka_bootstrap_servers_plaintext = spark.conf.get("kafka_bootstrap_servers_plaintext")
topic = spark.conf.get("topic")
startingOffsets = spark.conf.get("startingOffsets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dependencies
# MAGIC 
# MAGIC Here we will declare all of our library dependencies. It's generally considered best practice to keep these all together somewhere in the notebook to make for easy reference.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import dlt
import json

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Live Table 1: The Landing Table
# MAGIC 
# MAGIC For the sake of idempotency later on, it's important to trap the data in it's raw state. This reduces the likelihood of error down the road and reduces the need to subscribe and tap the stream multiple times.

# COMMAND ----------

kafka_events = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", topic )
  .option("startingOffsets", startingOffsets )
  .option("failOnDataLoss", False)
  .load()
    )

@dlt.table(comment="Raw DTS Data Capture", table_properties={"pipelines.reset.allowed": "true"})
def ad_dlt_dts_raw():
  return kafka_events
