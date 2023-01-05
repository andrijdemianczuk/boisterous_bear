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
# MAGIC ## Dependencies
# MAGIC 
# MAGIC Here we will declare all of our library dependencies. It's generally considered best practice to keep these all together somewhere in the notebook to make for easy reference.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load the current view of Live tables
# MAGIC This step loads whatever the latest view of the data is in the live tables. Remember that live tables are unbounded, so that means that they are constantly receiving new data. This is allows us to query the most up-to-date data that's process by the DLT pipeline.

# COMMAND ----------

df = spark.table("hive_metastore.ademianczuk.ad_dlt_dts_seq").filter(F.col("well") == "01-01P").drop("well","coordinates")
pdf = df.sort(F.col("timestamp").desc()).limit(60).drop("timestamp").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reviewing the original data source
# MAGIC 
# MAGIC Let's quickly review the sample DTS temperature chart that plots temp readings across the span according to line depth:
# MAGIC 
# MAGIC <img src="/files/Users/andrij.demianczuk@databricks.com/resources/Screenshot_2022_12_12_at_1_47_25_PM.png" width=750/>
# MAGIC 
# MAGIC Based on this visual representation, we segmented the above chart into logical parts as an approximation of real data. Clearly our simulated data is less variable, but for the sake of demonstration we are going to assume a similar shape so we can extend our persective by time as a third dimension:
# MAGIC 
# MAGIC <img src="/files/Users/andrij.demianczuk@databricks.com/resources/Screenshot_2022_12_12___segmented.jpg" width=750 />

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Taking a singular point-in-time representation of the demo data
# MAGIC 
# MAGIC Before we add the time dimension to the data, we will do a quick render based on the melted data to preview what our line graph looks like with the simulated data.

# COMMAND ----------

samp = spark.table("hive_metastore.ademianczuk.ad_dlt_dts_melted")

samp = samp.withColumn("timestamp", F.unix_timestamp(F.col("timestamp")).alias("timestamp"))
samp = samp.filter(F.col("timestamp") == 1672863962)
samp = samp.withColumn("segment", F.regexp_replace("segment", "seg_", ""))
samp = samp.withColumn("segment", F.col("segment").cast(IntegerType()))
samp = samp.orderBy(F.col("segment").asc())

# COMMAND ----------

display(samp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Presenting the data
# MAGIC 
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/8/8a/Plotly-logo.png/1200px-Plotly-logo.png" width=300 />
# MAGIC 
# MAGIC 
# MAGIC Since we removed a lot of the logic used to clean, munge and organize our data out to the DLT pipeline we now just need to focus on the presentation of the data. Since we are concerned with analyzing the data across three dimensions (segment length, temperature and time) we can easily transform the data to a vector that can be rendered by the plotly library.
# MAGIC 
# MAGIC For the sake of this demo, we are zeroing in on a single well on pad 01. This can also easily be parameterized if need be.

# COMMAND ----------

import plotly.graph_objects as go

fig = go.Figure(data=[go.Surface(z=pdf.values)])
fig.update_layout(title='DTS 10-hr History - Well 01-01P',
                  margin=dict(l=0, r=0, b=0, t=50), 
                  width=1000, height=750,
                 scene=dict(
                            xaxis_title='Distance - Meters',
                            yaxis_title='Hour',
                            zaxis_title='Temperature - Deg C',
                            ))

fig.show()
