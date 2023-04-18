# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Setup required data sets and rules

# COMMAND ----------

# MAGIC %run ./util/notebook_config

# COMMAND ----------

ddls = [
f"""DROP SCHEMA IF EXISTS {getParam('db')} CASCADE""",
f"""CREATE SCHEMA IF NOT EXISTS {getParam('db')} LOCATION '{getParam('data_path')}'""" ,
f"""USE {getParam('db')}""" 
]

for d in ddls:
  print(d)
  spark.sql(d)

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC mkdir /dbfs/tmp/rules
# MAGIC cd /dbfs/tmp/rules
# MAGIC pwd
# MAGIC echo "Removing all files"
# MAGIC rm -rf *
# MAGIC echo
# MAGIC wget https://raw.githubusercontent.com/lipyeow-lim/security-datasets01/main/forensics-2021/logs.zip
# MAGIC 
# MAGIC unzip logs.zip
# MAGIC 
# MAGIC ls -lR

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import *

# Load the zeek logs extracted from pcaps
for t in getParam('tables'):
  tb = f"{getParam('db')}.{t}"
  for f in getParam('folders'):
    jsonfile=f"{getParam('download_path')}/{f}/{t}.log"
    print(f"Loading {jsonfile} into {tb} ...")
    df = spark.read.format("json").load(jsonfile).withColumn("eventDate", col("ts").cast("Timestamp").cast("Date"))
    df.write.option("mergeSchema", "true").partitionBy("eventDate").mode("append").saveAsTable(tb)


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC USE rules_lipyeow_lim

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from kerberos

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select window, `id.orig_h`, `id.resp_h`, count(*), array_agg(uri) as uri, array_agg(user_agent) as user_agent
# MAGIC from http
# MAGIC --where date_trunc('HOUR', ts::TIMESTAMP) = to_unix_timestamp('2021-12-03T19:00:00.000+000')
# MAGIC group by window(ts::TIMESTAMP, '30 minutes' ), `id.orig_h`, `id.resp_h`

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select *
# MAGIC from dns

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC 
# MAGIC select window, `id.orig_h`, `id.resp_h`, count(*), array_distinct(array_agg(struct(query, answers))) as query_ans
# MAGIC from dns
# MAGIC group by window(ts::TIMESTAMP, '10 minutes' ), `id.orig_h`, `id.resp_h`

# COMMAND ----------


