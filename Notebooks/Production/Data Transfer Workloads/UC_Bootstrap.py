# Databricks notebook source
# MAGIC %md
# MAGIC ## Connection Bootstrap Notebook
# MAGIC 
# MAGIC Since our DLT Pipeline is streaming (always on & never actually finishes) if we add it as a normal DLT task to the workflow the child tasks will never execute. To get around this, we can program it asynchronously with a Jobs API call.
# MAGIC 
# MAGIC Since the downstream migration tables don't actually require the source stream to be up (assuming the tables were at least once written to) then they can just wait for the new events to come in whenever that may be.

# COMMAND ----------

# MAGIC %md
# MAGIC ```bash
# MAGIC #Maybe use a db secret
# MAGIC export DATABRICKS_TOKEN=dapi365fe47d8e9bcb38d943111216f91822
# MAGIC 
# MAGIC curl --netrc -X POST --header "Authorization: Bearer $DATABRICKS_TOKEN" \
# MAGIC https://e2-demo-field-eng.cloud.databricks.com/api/2.0/pipelines/9432993f-bdec-40cf-bbe2-68a00efed9b1/updates \
# MAGIC --data '{ "full_refresh": "false" }'
# MAGIC ```

# COMMAND ----------

# MAGIC %sh
# MAGIC export DATABRICKS_TOKEN=dapi365fe47d8e9bcb38d943111216f91822
# MAGIC 
# MAGIC curl --netrc -X POST --header "Authorization: Bearer $DATABRICKS_TOKEN" \
# MAGIC https://e2-demo-field-eng.cloud.databricks.com/api/2.0/pipelines/9432993f-bdec-40cf-bbe2-68a00efed9b1/updates \
# MAGIC --data '{ "full_refresh": "false" }'
