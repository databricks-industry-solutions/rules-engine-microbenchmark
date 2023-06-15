# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # IDE for rules engineering & testing

# COMMAND ----------

# MAGIC %run ./util/notebook_config

# COMMAND ----------

# Take a list of rules and generate the UDF code for checking them
def generate_udf_code(queries):
  rule_str = ""
  for q in queries:
    alert_str = json.dumps(json.dumps({ "id": q["id"], "desc": "bad alert", "rule": q["py"] }))
    rule=f"""  if {q['py'].replace('m[', 'rec[')}:
    result.append({alert_str})
"""
    rule_str += rule
  return f"""
def udf_detect(rec_str):
  rec = json.loads(rec_str)
  result = []
{rule_str}  
  if len(result)==0:
    return None
  return json.dumps(result)  
  """

# COMMAND ----------

import time
from pyspark.sql.types import *

query_str = f"m['method'] == 'GET' and 'pinrulesstl.cab' in m['uri']"
query_rec = {
  "id": 999,
  "py": query_str,
}
test_view = f"http"
nr = 1000

start_ts = time.time()
udf_code = generate_udf_code([query_rec])
#print(udf_code)
# execute the definition of the UDF definition
exec(udf_code)
spark.udf.register("udf_detect", udf_detect, StringType())
sql = f"""
select alerts
from (
  select udf_detect(raw) as alerts
  from (
    select to_json(struct(*)) as raw 
    from {test_view} 
    limit {nr}
  )
)
where alerts is not null
"""

df = spark.sql(sql)
print(df.count())
end_ts = time.time()
run_time = end_ts - start_ts
print(f"run time = {run_time} s")
display(df)

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from http;

# COMMAND ----------


