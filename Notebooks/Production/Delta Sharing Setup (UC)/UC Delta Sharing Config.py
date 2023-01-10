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
# MAGIC ## Delta Sharing
# MAGIC 
# MAGIC <img src="https://docs.databricks.com/_images/delta-sharing.png" width=1000/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Open sharing versus Databricks-to-Databricks sharing
# MAGIC The way you use Delta Sharing depends on who you are sharing data with:
# MAGIC 
# MAGIC * Open sharing lets you share data with any user, whether or not they have access to Databricks.
# MAGIC * Databricks-to-Databricks sharing lets you share data with Databricks users who have access to a Unity Catalog metastore that is different from yours.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Setting up Delta Sharing
# MAGIC 
# MAGIC Setting up Delta sharing is a fairly straightforward process. It consists of four parts, and tables to be in Delta Format in a Unity Catalog metastore.
# MAGIC <br/>
# MAGIC <br/>
# MAGIC 1. Enable Delta Sharing on your account
# MAGIC 1. Enable Delta Sharing on your metastore
# MAGIC 1. Create and manage share volumes
# MAGIC 1. Create and manage recipients associated with share volumes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting up a share

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new share if none exists
# MAGIC CREATE SHARE IF NOT EXISTS ademianczuk_dts_external
# MAGIC COMMENT "SAGD DTS Data Share for open sharing";

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How to view the details of the share
# MAGIC DESCRIBE SHARE ademianczuk_dts_external

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How to see shares with a match condition
# MAGIC SHOW SHARES LIKE "ademianczuk_dts*"

# COMMAND ----------

# MAGIC %sql
# MAGIC -- If an alias is used, the consumer must use the alias - the original table name won't work if this option is set.
# MAGIC ALTER SHARE ademianczuk_dts_external
# MAGIC ADD TABLE field_demos.canwest_sa.ad_dts2_sharing_seq
# MAGIC AS ademianczuk_dts_external.dts_open_share;
# MAGIC 
# MAGIC ALTER SHARE ademianczuk_dts_external
# MAGIC ADD TABLE field_demos.canwest_sa.ad_dts2_sharing_melted
# MAGIC AS ademianczuk_dts_external.dts_open_melted

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW ALL IN SHARE ademianczuk_dts_external

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting up a recipient
# MAGIC 
# MAGIC For this demo, we will be setting up an external recipient. The main difference between an internal and external recipient is the use of an activation link for external recipients or a Databricks UID for internal recipients.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE RECIPIENT IF NOT EXISTS ademianczuk_dts
# MAGIC COMMENT "External Recipient for the dts delta share";

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DESCRIBE RECIPIENT ademianczuk_dts

# COMMAND ----------

# MAGIC %md
# MAGIC ### The activation link
# MAGIC <img src="/files/Users/andrij.demianczuk@databricks.com/resources/Screenshot_2023_01_10_at_11_19_41_AM.png" width=1000/>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### The credential file
# MAGIC <img src="/files/Users/andrij.demianczuk@databricks.com/resources/Screenshot_2023_01_10_at_12_40_59_PM.png" width=500 />

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC GRANT SELECT ON SHARE ademianczuk_dts_external
# MAGIC TO RECIPIENT ademianczuk_dts

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANTS TO RECIPIENT ademianczuk_dts

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW GRANT ON SHARE ademianczuk_dts_external
