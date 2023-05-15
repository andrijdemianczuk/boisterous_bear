# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://delta.io/static/delta-sharing-logo-13e769d1a6148b9cc2655f97e90feab5.svg" />

# COMMAND ----------

# DBTITLE 1,First, let's make sure the dependencies are installed. This can also be done at the cluster-level too.
# MAGIC %pip install delta-sharing

# COMMAND ----------

#Bring the library in scope
import delta_sharing

# COMMAND ----------

# DBTITLE 1,Identify where we stored the downloaded credentials file
#This file was placed in the following location after we downloaded the file when we created the receiver.
creds = "dbfs:/Users/andrij.demianczuk@databricks.com/resources/ademianczuk_ext_dts.share"

# COMMAND ----------

# DBTITLE 1,Let's quickly preview the credentials to make sure they're legit
# MAGIC %fs head /Users/andrij.demianczuk@databricks.com/resources/ademianczuk_ext_dts.share

# COMMAND ----------

# DBTITLE 1,Test the connection, and view the shared objects
#Location of our profile credentials
#This is the same location as above, but with the file API format instead
profile_file = '/dbfs/Users/andrij.demianczuk@databricks.com/resources/ademianczuk_ext_dts.share'

# Create a SharingClient
client = delta_sharing.SharingClient(profile_file)

# List all shared tables.
client.list_all_tables()

# COMMAND ----------

# DBTITLE 1,Format the output of the share objects - this is just for readability
shares = client.list_shares()

for share in shares:
  schemas = client.list_schemas(share)
  for schema in schemas:
    tables = client.list_tables(schema)
    for table in tables:
      print(f'name = {table.name}, share = {table.share}, schema = {table.schema}')

# COMMAND ----------

# DBTITLE 1,Define the location of the credentials and table we want to use
profile_file = '/Users/andrij.demianczuk@databricks.com/resources/ademianczuk_ext_dts.share'
table_url = f"{profile_file}#ademianczuk_dts_external.ademianczuk_dts_external.dts_open_daily_avg"

# COMMAND ----------

# DBTITLE 1,Using the location and credentials, let's create a simple dataframe from the delta share we have access to
#Now let's view our dataframe we would be using for further ETL / enrichment
from pyspark.sql.functions import sum, col, count, avg

#This includes the location of the delta sharing server, auth token and table we want to query.
df = delta_sharing.load_as_spark(table_url)

display(df.groupBy(df.segment).agg(avg(df.avg_temp).alias("average_temp")).orderBy(df.segment))
