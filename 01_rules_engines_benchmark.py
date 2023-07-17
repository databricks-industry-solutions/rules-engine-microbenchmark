# Databricks notebook source
# MAGIC %md
# MAGIC # Micro-benchmark for Analyzing Rules Engine Options
# MAGIC
# MAGIC This is a standalone notebook.
# MAGIC
# MAGIC ## Goal
# MAGIC
# MAGIC Find the most efficient and lowest cost (DBUs) approach for implementing a rules engine (eg. detection engine) for cybersecurity applications.
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/lipyeowlim/public/main/img/fusion-rules/rules_engine.png" width="650">
# MAGIC
# MAGIC ## Rules engine assumptions
# MAGIC * Input: 
# MAGIC   * table of rules (antecedent, consequent) that can be updated by users at any time
# MAGIC   * table/stream of "fused data" - convert to JSON format
# MAGIC * Output:
# MAGIC   * table/stream of rule triggers/hits - each hit includes rule ID, antecedent, raw data for traceability
# MAGIC   
# MAGIC ## Requirements
# MAGIC
# MAGIC * latency - amenable to parallelization by Spark.
# MAGIC * scalability in number of rules
# MAGIC * cost
# MAGIC * supports both batch and streaming modes
# MAGIC
# MAGIC ## Use cases
# MAGIC 1. Detection engine in an XDR-like scenario - typically a few thousand rules. Some rules to be applied at 5m, 15m, 60m, 24h periodicity
# MAGIC 1. Auto-disposition engine in an XDR/SOAR-like scenario - applied to alerts to auto-disposition known true positive or false positive conditions
# MAGIC 1. Alerting for fusion-center fraud detection or other fusion analytics.
# MAGIC
# MAGIC ## Rules Engine Implementation Options
# MAGIC
# MAGIC 1. Union-All SQL query (straw man)
# MAGIC 2. Case-statement SQL query (with where-clause)
# MAGIC 3. Durable rules package (Rete's Algorithm implemented in C) - not easily parallelizable.
# MAGIC 4. Dynamically generated UDF (python and pandas)

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC %pip install durable_rules

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Generates Benchmark Data and Rules
# MAGIC
# MAGIC * A pool of `max_values` text values is generated using `Faker` with each value conforming to a specified `max_value_length`.
# MAGIC * The event data is generated as a row of text fields. Each field is randomly chosen from the pool of text values. 
# MAGIC * Each rule/query is generated as a conjunction of at most `max_terms` predicates. Each predicate being of the form `column_id = value`. The columns are randomly chosen and the value is randomly chosen from the pool of values.

# COMMAND ----------

# DBTITLE 1,Setup widgets for feature flags
dbutils.widgets.removeAll()
flags = { 
         "Load data": True,
         "Load queries": True, 
         "Run union all SQL": False, 
         "Run case SQL": False, 
         "Run durable rules": False
        }
valmap = { "True": True, "False": False }
for (wid, _) in flags.items():
  dbutils.widgets.dropdown(wid, str(flags[wid]), ["True", "False"])
  flags[wid] = valmap[dbutils.widgets.get(wid)]

# COMMAND ----------

# DBTITLE 1,Setup config parameters
from faker import Faker
import random
import json
import time
from pyspark.sql.types import *
import pandas as pd
from typing import Iterator
from pyspark.sql.functions import col, pandas_udf, struct
import uuid

cfg = {
  "db": "lipyeow_ctx",
  "events": "fake_events",
  "detections": "detections",
  # Event data generation params
  "col_prefix": "c",
  "ncols": 20,
  "nrows": 400000,
  "max_values": 100,
  "max_value_len": 30,
  "generate_data": flags["Load data"],
  # Rules/queries generation params
  "nqueries": 5000,
  "max_terms": 10,
  "generate_queries": flags["Load queries"],
  # The rule set size to run the benchmark on
  "test_nq": [500, 1000],
  # The number of rows to run the detection on
  "test_nr": [50000, 100000],
  # The rules engines to run the benchmark on
  "run_union_all_sql": flags["Run union all SQL"],
  "run_case_sql": flags["Run case SQL"],
  "run_durable_rules": flags["Run durable rules"]
}

cfg["cols"] = [ cfg["col_prefix"] + "{:03d}".format(i) for i in range(cfg["ncols"]) ]

col_specs = ", ".join([ c + " string" for c in cfg["cols"]])
cfg["ddl"]  = f"CREATE TABLE IF NOT EXISTS {cfg['db']}.{cfg['events']} ( rid int, {col_specs} )"

print(cfg["cols"])
print(cfg["ddl"])

fake = Faker()
Faker.seed(0)

cfg["values"] = [ fake.text(max_nb_chars=cfg["max_value_len"]) for i in range(cfg["max_values"]) ]

sql=f"create database if not exists {cfg['db']}"
print(sql)
spark.sql(sql)


# COMMAND ----------

# DBTITLE 1,Utility function definitions
def generate_row(cfg):
  return [ random.choice(cfg["values"]) for _ in range(cfg["ncols"]) ]

def generate_insert(cfg, batch_id, ntuples):
  tuples = []
  for i in range(ntuples):
    rid = batch_id * ntuples + i
    r = [ f"'{x}'" for x in generate_row(cfg) ]
    tuples.append( "\n(" + str(rid) + "," + ",".join(r) + ")" )
  ins = f"insert into {cfg['db']}.{cfg['events']} values { ','.join(tuples)}"
  return ins

def generate_query(cfg):
  n_terms = random.randint(2, cfg["max_terms"])
  q = []
  for col in random.sample(cfg["cols"], n_terms):
    val = random.choice(cfg["values"])
    q.append([col, val])
  return q

def query_to_sql_where(q):
  where = " AND ".join([ f"{col} = '{val}'" for (col, val) in q])
  return where

def query_to_rule(q):
  where = " & ".join([ f"(m.{col} == '{val}')" for (col, val) in q])
  return where

def query_to_py(q):
  where = " and ".join([ f"""(m["{col}"] == "{val}")""" for (col, val) in q])
  return where

def generate_query_workload(cfg):
  result = []
  for i in range(cfg["nqueries"]):
    q = generate_query(cfg)
    sql_where = query_to_sql_where(q)
    py_if = query_to_py(q)
    rule = query_to_rule(q)
    result.append({"id": i, "sql": sql_where, "rule": rule, "py": py_if})
  return result

def generate_union_all_sql(cfg, queries):
  sql_queries = []
  for q in queries:
    sql = f"""select {q['id']} as detection_id, to_json(struct(*)) as raw 
from {cfg['db']}.{cfg['events']}
where {q['sql']}"""
    sql_queries.append(sql)
  return "\n\nunion all\n\n".join(sql_queries)

def generate_case_sql(cfg, queries):
  where = []
  case = [] 
  for q in queries:
    where.append("\n  (" + q["sql"] + ")\n")
    case.append(f"    when {q['sql']} then {q['id']}")
  case_str = "case\n" + "\n\n".join(case) + "\n    else null\n  end as detection_id"
  sql = f"""
select 
  {case_str},
  to_json(struct(*)) as raw
from {cfg['db']}.{cfg['events']}
where {'  or '.join(where)}
"""  
  return sql

def generate_rules_json(cfg, queries):
  rules = []
  for q in queries:
    obj = { "antecedent": q["rule"],
            "consequent": {
              "disposition": "true positive"
            }
          }
    rules.append(obj)
  return rules

# this function has side effect and is destructive!
def reload_data(cfg):
  ddl = f"drop table if exists {cfg['db']}.{cfg['events']}"
  spark.sql(ddl)
  spark.sql(cfg["ddl"])
  batch_size = 10000
  nbatches = int(cfg["nrows"] / batch_size)
  for i in range(nbatches):
    print(f"ins batch #{str(i)}")
    ins = generate_insert(cfg, i, batch_size)
    spark.sql(ins)

# this function has side effect and is destructive!
def reload_queries(cfg):
  sql_list = [
    f"drop table if exists {cfg['db']}.{cfg['detections']};",
    f"create table if not exists {cfg['db']}.{cfg['detections']}(id int, sql string, rule string, py string, consequent string)"
  ]
  for sql in sql_list:
    print(sql)
    spark.sql(sql)
    
  new_queries = generate_query_workload(cfg)
  vlist=[]
  for q in new_queries:
    sql = q['sql'].replace("'", "\\'")
    rule = q['rule'].replace("'", "\\'")
    val = f"""({q['id']}, "{sql}", "{rule}", '{q['py']}', null)"""
    vlist.append(val)

  ins = f"""insert into {cfg['db']}.{cfg['detections']} values {','.join(vlist)}"""  
  spark.sql(ins)

# Take a list of rules and generate the UDF code for checking them
def generate_udf_code(queries):
  rule_str = ""
  for q in queries:
    alert_str = json.dumps({ "id": q["id"], "desc": "bad alert", "rule": q["py"] })
    rule=f"""  if {q['py'].replace('m[', 'rec[')}:
    result.append('{alert_str}')

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

# session-level 
# metrics list of lists (id string, ncols int, nrows int, nqueries int, run_ts float)
# ["case_sql", cfg["ncols"], cfg["nrows"], 1000, 80.0]
metrics = []

# COMMAND ----------

# DBTITLE 1,Sanity tests for utility functions
i = 0
for _ in range(5):
  value = random.choice(cfg["values"])
  #value = fake.word()
  print(f"{str(i)}: Picking value = {value}")
  row = generate_row(cfg)
  #print(row)
  q = generate_query(cfg)
  #print(q)
  #print(query_to_sql(q))
  #print(query_to_rule(q))
  i += 1
  
queries = generate_query_workload(cfg)

print("=========\nQueries\n=========")
print(json.dumps(queries[:3], indent=2))

print("\n=========\nUnion all SQL\n=========\n")
print( generate_union_all_sql(cfg, queries[:3]))

print("\n=========\nCase SQL\n=========\n")
print( generate_case_sql(cfg, queries[:3]))

print("\n=========\nUDF SQL\n=========\n")
print( generate_udf_code(queries[:3]))

print("\n=========\nInsert SQL\n=========")
print( generate_insert(cfg, 1, 3))


# COMMAND ----------

# DBTITLE 1,Generate data & load into data table
if cfg["generate_data"]:
  reload_data(cfg)

# COMMAND ----------

# DBTITLE 1,Sanity check the generated data
sql = f"select * from {cfg['db']}.{cfg['events']} limit 5"

print(sql)
display(spark.sql(sql))

sql = f"""
select total_bytes, total_rows, total_bytes/total_rows as bytes_per_row
from (
  select sum(row_bytes) as total_bytes, count(*) as total_rows
  from (
    select octet_length( to_json(struct(*)) ) as row_bytes 
    from {cfg['db']}.{cfg['events']} 
  )
)
"""

print (sql)
display(spark.sql(sql))

# COMMAND ----------

# DBTITLE 1,Generate rules and load into detections table

if cfg["generate_queries"]:
  reload_queries(cfg)

# COMMAND ----------

# DBTITLE 1,Sanity check the detections table
sql = f"select * from {cfg['db']}.{cfg['detections']} limit 3"

print(sql)
display(spark.sql(sql))

# COMMAND ----------

# DBTITLE 1,Read test rules into memory
nq = cfg['nqueries']
#nq = 3
df = spark.sql(f"""
select array_agg(q_json) as q_list 
from (
  select to_json(struct(*)) as q_json 
  from {cfg['db']}.{cfg['detections']}
  where id < {nq}
  order by id
)""")

test_queries = []
for q_str in df.first().q_list:
  q_rec = json.loads(q_str)
  test_queries.append(q_rec)

print(json.dumps(test_queries[:2], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC # Option 1: Union All SQL
# MAGIC
# MAGIC * Semantic: No early termination - all rules will be checked
# MAGIC * SQL based
# MAGIC * Observations:
# MAGIC   * The optimizer does not automatically do multi-query optimization.
# MAGIC   * Extremely slow and is deprecated

# COMMAND ----------

if cfg["run_union_all_sql"]:
  for nq in cfg["test_nq"]:
    start_ts = time.time()
    sql_str = generate_union_all_sql(cfg, test_queries[:nq])
    df = spark.sql( sql_str )
    print(df.count())
    end_ts = time.time()
    run_time = end_ts - start_ts
    print(f"run time = {run_time} s")
    metrics.append(["union_all_sql", cfg["ncols"], cfg["nrows"], nq, run_time])
  display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Option 2: Case Statement SQL
# MAGIC
# MAGIC * Semantic: Early termination after highest priority rule fires
# MAGIC * SQL based
# MAGIC * Parallelizable
# MAGIC * Observations:
# MAGIC   * Decent performance
# MAGIC   * At least it is doing a single pass over the data to process all the rules/queries
# MAGIC   * Not extensible to the case where there is no early termination.

# COMMAND ----------


if cfg["run_case_sql"]:
  for nq in cfg["test_nq"]:
    start_ts = time.time()
    sql_str = generate_case_sql(cfg, test_queries[:nq])
    df = spark.sql( sql_str )
    print(df.count())
    end_ts = time.time()
    run_time = end_ts - start_ts
    print(f"run time = {run_time} s")
    metrics.append(["case_sql", cfg["ncols"], cfg["nrows"], nq, run_time])
  
  display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Option 3: `durable_rules` python package 
# MAGIC
# MAGIC * Semantic: Early termination after highest priority rule fires
# MAGIC * Based on durable_rules package that has a C-based engine
# MAGIC * Able to do stateful forward-chaining inferencing (can be applied to auto APT attribution?)
# MAGIC * No easy way to parallelize using UDF or spark at the moment - will be executed in-memory on the driver node.
# MAGIC * Observations:
# MAGIC   * Starts to hang when number of rules > 500
# MAGIC   * The state of the durable engines is held in memory and that is a limitation

# COMMAND ----------

from durable.lang import *
import json
import jinja2
from pprint import pprint
from pyspark.sql.types import *

# these templates would be from the reporting templates database
templates={ "t1": "findings: {{alert.name}}", "t2": "malware: {{alert.id}}" }

# Leveraging the durable_rules state in c.s.myvar to store some results of the actions.
def process_consequent_actions(c, action_str, num):
    #print("-----------\naction #" + str(num) + "\n-----------\n")
    actions = json.loads(action_str)
    alert = c.m
    #print("state:")
    #print(c.s)
    if c.s.myvar is None:
      c.s.myvar = []
    c.s.myvar.clear()
    for k,v in actions.items():
        if k=="disposition":
            result_str = "setting disposition to " + v
        elif k=="auto-report":
            template_id = v["template_id"]
            assert template_id in templates
            t = jinja2.Template(templates[template_id])
            result_str = "sending report: " + t.render(alert=alert)
        else:
            result_str = "unsupported action: " + k 
            #df = spark.sql("select 'world' as hello")
            #result_str += " - " + df.first().hello
        #print(result_str)
        c.s.myvar.append(result_str)
    return result_str
  
def gen_rule_str(rule, r):
#    rule_str=f'''
#@when_all({rule["antecedent"]})
#def action_{str(r)}(c):
#    print ('action_{str(r)}')
#'''
    rule_str='''
@when_all({0})
def action_{1}(c):
    return process_consequent_actions(c, {2}, {1})
'''.format(rule["antecedent"], str(r), json.dumps(json.dumps(rule["consequent"])))
 
    return rule_str

def create_ruleset(ruleset_name, rules):
    r = 0
    with ruleset(ruleset_name):
        for rule in rules:
            #print(gen_rule_str(rule, r))
            exec(gen_rule_str(rule, r))
            r += 1
            
def print_ruleset(rules):
  r = 0
  for rule in rules:
    print(gen_rule_str(rule, r))
    r += 1
            
def detect(ruleset_name, rec_str):
  rec = json.loads(rec_str)
  state = None
  try:
    state = post(ruleset_name, rec) 
    #print(state)
  except Exception as e:
    #print("no match")
    #return str(e)
    return None
  if state is not None:
    return json.dumps(state["myvar"])  
  return "[state is None]"


# COMMAND ----------

# DBTITLE 1,Setup the rules
nq = cfg["test_nq"][0]
if nq>500:
  cfg["run_durable_rules"] = False
else:
  rules_json = json.dumps(generate_rules_json(cfg, test_queries[:nq]), indent=2)
  print(rules_json)
  
if cfg["run_durable_rules"]:
  ruleset_name = uuid.uuid4()
  rules = json.loads(rules_json)
  #print_ruleset(rules)
  create_ruleset (ruleset_name, rules)


# COMMAND ----------

# DBTITLE 1,For debugging
debug_durable = False
if debug_durable:
  rec = { "c004": 'Computer on fast play fact.', "c005": 'Home deal important current.', "c000": 'Stand part us will.'}

  rec_str2 = json.dumps(rec)
  res = detect(ruleset_name, rec_str2)
  if res is not None:
    print (f"detect ({rec_str}) = {res}")


# COMMAND ----------

# DBTITLE 1,Actual checking of data against rules
 
sql = f"""
select to_json(struct(*)) as raw 
from {cfg['db']}.{cfg['events']} 
"""

if cfg["run_durable_rules"]:
  start_ts = time.time()
  df = spark.sql(sql)
  j=0
  for (rec_str,) in df.collect():
    #rec = json.loads(rec_str)
    #rec_str2 = json.dumps(rec)
    #print(rec_str)
    res = detect(ruleset_name, rec_str)
    if res is not None:
      #print(str(j))
      j += 1
      #print (f"detect ({rec_str}) = {res}")
    #print(rec_str)
    #print(rec_str2)
    #break
  print(str(j) + " alerts")
  end_ts = time.time()
  run_time = end_ts - start_ts
  print(f"run time = {run_time} s")
  metrics.append(["durable_rules", cfg["ncols"], cfg["nrows"], nq, run_time])


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Option 4: Python UDF (includes pandas UDF)
# MAGIC
# MAGIC * Semantic: No early termination - all rules will be checked. Early termination is an easy modification.
# MAGIC * Python UDF-based - can be parallelized easily by spark
# MAGIC * Not clear if pandas UDF will be any more efficient - TO INVESTIGATE
# MAGIC * Can be used in DLT & Streaming as well
# MAGIC * Relies on UDF code generation and dynamic execution of the function definition!
# MAGIC * If used in streaming mode, will need to restart the streaming job to update the UDF definition to pick up the latest rules

# COMMAND ----------

# DBTITLE 1,Generate detection UDF definition and check data against rules
for nr in cfg["test_nr"]:
  for nq in cfg["test_nq"]:
    start_ts = time.time()
    udf_code = generate_udf_code(test_queries[:nq])
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
    from {cfg['db']}.{cfg['events']} 
    where rid < {nr}
  )
)
where alerts is not null
"""

    df = spark.sql(sql)
    print(df.count())
    end_ts = time.time()
    run_time = end_ts - start_ts
    print(f"run time = {run_time} s")
    metrics.append(["udf_sql", cfg["ncols"], nr, nq, run_time])

display(df)


# COMMAND ----------

# DBTITLE 1,Checking to see if the batching in pandas_udf makes a difference
for nr in cfg["test_nr"]:
  for nq in cfg["test_nq"]:
    start_ts = time.time()
    # generate the udf code and wrap in a pandas_udf
    udf_code = generate_udf_code(test_queries[:nq])
    #print(udf_code)
    # execute the definition of the UDF definition
    exec(udf_code)
    def pd_detect(batch_iter: pd.Series) -> pd.Series:
      return batch_iter.apply(udf_detect)
    pd_udf_detect = pandas_udf(pd_detect, returnType=StringType())
    spark.udf.register("pd_udf_detect", pd_udf_detect)
    sql = f"""
select alerts
from (
  select pd_udf_detect(raw) as alerts
  from (
    select to_json(struct(*)) as raw 
    from {cfg['db']}.{cfg['events']} 
    where rid < {nr}
  )
)
where alerts is not null
"""
    df = spark.sql(sql)
    print(df.count())
    end_ts = time.time()
    run_time = end_ts - start_ts
    print(f"run time = {run_time} s")
    metrics.append(["pd_udf_sql", cfg["ncols"], nr, nq, run_time])

display(df)


# COMMAND ----------

# DBTITLE 1,Results from current run on current cluster
metrics_df = spark.createDataFrame(metrics, schema="id string, ncols int, nrows int, nqueries int, run_time double")
display(metrics_df)
print(metrics)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Sample results from 1-node clusters
# MAGIC
# MAGIC * Assumes same data size to be scanned even for different batch periods/frequencies
# MAGIC * Multi-tenancy approaches to be addressed in separate notebooks
# MAGIC * Cost estimates does not include discounts, special rates, optimizations (eg. leveraging spot instances), storage costs, ingest & ELT costs.

# COMMAND ----------

# DBTITLE 1,Overview results for 300 rules on 1-node i3.xlarge spark cluster
one_node_metrics = [
  ['union_all_sql', 20, 100000, 300, 507.4093222618103], 
  ['case_sql', 20, 100000, 300, 10.232491731643677], 
  ['durable_rules', 20, 100000, 300, 10.743492841720581], 
  ['udf_sql', 20, 100000, 300, 4.0141706466674805], 
  ['pd_udf_sql', 20, 100000, 300, 3.3554890155792236]]
one_node_metrics_df = spark.createDataFrame(one_node_metrics, schema="id string, ncols int, nrows int, nqueries int, run_time double")

display(one_node_metrics_df)

# COMMAND ----------

# DBTITLE 1,Scalability results on 1-node i3.2xlarge spark cluster
# 11.3 LTS (includes Apache Spark 3.3.0, Scala 2.12)
# i3.2xlarge: AWS $0.624 per hour (charged at second granularity) https://aws.amazon.com/ec2/pricing/on-demand/
# DB calc: https://www.databricks.com/product/pricing/product-pricing/instance-types

one_node_metrics = [
["case_sql",20,100000,500,15.810449838638306],
["case_sql",20,100000,1000,57.52072048187256],
["case_sql",20,100000,2000,236.84430360794067],
["case_sql",20,100000,4000,1027.3621261119843],
["udf_sql",20,100000,500,3.0255379676818848],
["udf_sql",20,100000,1000,3.76298189163208],
["udf_sql",20,100000,2000,6.916287422180176],
["udf_sql",20,100000,4000,16.512221336364746],
["pd_udf_sql",20,100000,500,2.91202712059021],
["pd_udf_sql",20,100000,1000,3.854057788848877],
["pd_udf_sql",20,100000,2000,6.9562201499938965],
["pd_udf_sql",20,100000,4000,16.615703105926514]
]

one_node_metrics_df = spark.createDataFrame(one_node_metrics, schema="id string, ncols int, nrows int, nqueries int, run_time double")

display(one_node_metrics_df)

# Batch detection every 5 minutes.
# AWS on-demand EC2 costs: 16 * 12 * 24 * 30 / 60 / 60 * .624 = $24 per month
# DB costs: 16 * 12 * 24 * 30 / 60 / 60 * 5.8 * .15 = $33.5 per month
# Total compute costs for detection = $58 per month

# COMMAND ----------

# DBTITLE 1,Scalability results on 1-node i3.xlarge spark cluster
one_node_metrics = [['udf_sql', 20, 50000, 500, 3.441438913345337], ['udf_sql', 20, 50000, 1000, 4.017544984817505], ['udf_sql', 20, 50000, 2000, 7.723710298538208], ['udf_sql', 20, 50000, 4000, 9.105348587036133], ['udf_sql', 20, 100000, 500, 4.231768846511841], ['udf_sql', 20, 100000, 1000, 6.751785039901733], ['udf_sql', 20, 100000, 2000, 11.992274522781372], ['udf_sql', 20, 100000, 4000, 16.867436170578003], ['udf_sql', 20, 200000, 500, 8.824966669082642], ['udf_sql', 20, 200000, 1000, 15.41950511932373], ['udf_sql', 20, 200000, 2000, 24.986423015594482], ['udf_sql', 20, 200000, 4000, 38.57883644104004], ['udf_sql', 20, 400000, 500, 15.520002603530884], ['udf_sql', 20, 400000, 1000, 22.030455589294434], ['udf_sql', 20, 400000, 2000, 39.17914652824402], ['udf_sql', 20, 400000, 4000, 66.15570950508118], ['pd_udf_sql', 20, 50000, 500, 2.839707851409912], ['pd_udf_sql', 20, 50000, 1000, 4.2576234340667725], ['pd_udf_sql', 20, 50000, 2000, 7.9649882316589355], ['pd_udf_sql', 20, 50000, 4000, 8.631979942321777], ['pd_udf_sql', 20, 100000, 500, 3.685722589492798], ['pd_udf_sql', 20, 100000, 1000, 6.035593032836914], ['pd_udf_sql', 20, 100000, 2000, 11.679226398468018], ['pd_udf_sql', 20, 100000, 4000, 16.876041412353516], ['pd_udf_sql', 20, 200000, 500, 7.437154054641724], ['pd_udf_sql', 20, 200000, 1000, 12.70432424545288], ['pd_udf_sql', 20, 200000, 2000, 23.248831510543823], ['pd_udf_sql', 20, 200000, 4000, 36.90821838378906], ['pd_udf_sql', 20, 400000, 500, 13.722443342208862], ['pd_udf_sql', 20, 400000, 1000, 20.945995092391968], ['pd_udf_sql', 20, 400000, 2000, 37.47307562828064], ['pd_udf_sql', 20, 400000, 4000, 64.08518719673157]]

one_node_metrics_df = spark.createDataFrame(one_node_metrics, schema="id string, ncols int, nrows int, nqueries int, run_time double")

display(one_node_metrics_df.where("nrows=100000"))
display(one_node_metrics_df.where("nqueries=4000"))

# COMMAND ----------

# DBTITLE 1,Utility to extract the runtime model from current run
pts = {}
for r in one_node_metrics:
  if r[2]==50000:
    if r[0] not in pts:
      pts[r[0]] = {}
    if r[3] not in pts:
      pts[r[0]][str(r[3])] = [ r[2], r[4] ]
  if r[2]==400000:
    pts[r[0]][str(r[3])].extend([r[2], r[4]])
print(json.dumps(pts, indent=2))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show functions like '*_detect'

# COMMAND ----------


