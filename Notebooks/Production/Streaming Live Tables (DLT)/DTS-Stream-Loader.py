# Databricks notebook source
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
schema_location = spark.conf.get("schema_location")
