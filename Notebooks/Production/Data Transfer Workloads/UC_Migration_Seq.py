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

#Open the stream from the live table and write it to the UC location

(spark.readStream
      .format("delta")
#       .option("ignoreChanges", "true")
      .table("hive_metastore.ademianczuk.ad_dlt_dts2_seq")
      .writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", "/FileStore/Users/andrij.demianczuk@databricks.com/tmp/dts_seq_cp")
      .toTable("field_demos.canwest_sa.ad_dts2_sharing_seq")
)
      
#simple right?
