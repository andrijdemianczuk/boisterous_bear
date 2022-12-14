# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

startingOffsets = "latest" 
kafka_bootstrap_servers_plaintext = "35.86.112.176:9092"
topic = "dts-test1"

kafka = (spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext ) 
  .option("subscribe", topic )
  .option("startingOffsets", startingOffsets )
  .load())

# COMMAND ----------

read_stream = kafka.select(col("value"))
df = kafka.select(col("value").cast("string").alias("plaintextValue"))

# COMMAND ----------

path = "/FileStore/Users/andrij.demianczuk@databricks.com/tmp/kafka"
checkpointPath = "/FileStore/Users/andrij.demianczuk@databricks.com/tmp/kafka_cp"
df.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpointPath).toTable("field_demos.canwest_sa.ad_dts_raw")

# COMMAND ----------

df1 = spark.table("field_demos.canwest_sa.ad_dts_raw")

# COMMAND ----------

# import json
# from pyspark.sql.types import *

# # Define the schema
# schema = StructType(
#     [StructField("name", StringType(), True), StructField("age", IntegerType(), True)]
# )

# # Write the schema
# with open("schema.json", "w") as f:
#     json.dump(schema.jsonValue(), f)

# # Read the schema
# with open("schema.json") as f:
#     new_schema = StructType.fromJson(json.load(f))
#     print(new_schema.simpleString())

# COMMAND ----------

sample_loc = "/FileStore/Users/andrij.demianczuk@databricks.com/tmp/kafka/schemas/dts-test1.json"
json_schema = spark.read.json(df1.rdd.map(lambda row: row.plaintextValue)).schema

# dbutils.fs.put(schema_loc, str(json_schema))

# COMMAND ----------

df2 = df1.withColumn('json', from_json(col('plaintextValue'), json_schema))

# COMMAND ----------

#dbutils.fs.ls("/FileStore/Users/andrij.demianczuk@databricks.com/tmp/kafka/schemas/")
dbutils.fs.rm("/FileStore/Users/andrij.demianczuk@databricks.com/tmp/kafka/schemas/", True)

# COMMAND ----------

display(df2.select(col("json")))

# COMMAND ----------

df2 = df2.select("json.*")
