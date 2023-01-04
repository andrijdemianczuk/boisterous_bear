# Databricks notebook source
from pyspark.sql.functions import *
from time import *

latest = time()
earliest = latest - 1800

# COMMAND ----------

dts = spark.table("ademianczuk.ad_dts_raw").where(col("well") == "01-01P")

# COMMAND ----------

dts_q = spark.table("field_demos.canwest_sa.ad_dts_json").where(col("well") == "01-01P").where(col("timestamp")>= earliest).drop("well", "coordinates", "timestamp")

# COMMAND ----------

#df = df.filter(col('timestamp') <= date_sub(current_date(), 0)).filter(col('timestamp') >= date_sub(current_date(), 1))
display(dts_q)

# COMMAND ----------

# dts_wide = spark.table("field_demos.canwest_sa.ad_dts_json").where(col("well") == "01-01P")
# display(dts_wide.groupBy(col("well")).count())

# COMMAND ----------

dts = dts.drop("well", "coordinates", "sha2")

# COMMAND ----------

display(dts)

# COMMAND ----------

display(dts.count())

# COMMAND ----------

import plotly.graph_objects as go

import pandas as pd

# Read data from a csv
z_data = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/api_docs/mt_bruno_elevation.csv')

fig = go.Figure(data=[go.Surface(z=pdf.values)])

fig.update_layout(title='Mt Bruno Elevation',
                  margin=dict(l=0, r=0, b=0, t=0))

fig.show()

# COMMAND ----------

display(z_data)

# COMMAND ----------

import plotly.express as px
df = px.data.iris()
fig = px.scatter_3d(df, x='sepal_length', y='sepal_width', z='petal_width',
                    color='petal_length', symbol='species')
fig.update_layout(margin=dict(l=0, r=0, b=0, t=0))
fig.show()

# COMMAND ----------

display(df)

# COMMAND ----------

import plotly.graph_objects as go

import pandas as pd

# Read data from a csv
z_data = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/api_docs/mt_bruno_elevation.csv')

fig = go.Figure(data=[go.Surface(z=z_data.values)])

fig.update_layout(title='Mt Bruno Elevation', autosize=True,
                  margin=dict(l=0, r=0, b=0, t=0))

fig.show()

# COMMAND ----------

from datetime import datetime, timedelta

end = datetime.today() - timedelta(hours=31, minutes=0)
start = datetime.today() - timedelta(hours=31, minutes=10)
print(end - start)

# COMMAND ----------

dts = dts.filter(col("timestamp")>= start).filter(col("timestamp")<= end)

# COMMAND ----------

dts.count()

# COMMAND ----------

pdf = dts_q.toPandas()

# COMMAND ----------


