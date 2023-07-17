# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Rules engine cost estimator for single-node compute
# MAGIC
# MAGIC The cost model
# MAGIC * is currently limited to single-node clusters
# MAGIC * the processing time for the rules engine is within the period of the batch job
# MAGIC * assumes that the event data is ingested at a constant rate in rows per minute
# MAGIC   * Moreover, the event data is already in the Databricks workspace.
# MAGIC   * The data rate is specified in the input widget to the model.
# MAGIC   * If there are 100K rows to be processed in a 5min batch process, there will be 200K rows to be processed if you were to use a 10min batch process instead.
# MAGIC   * Using the default data generation config, each row is approximately 692 bytes, so 20K rows per minute corresponds to 13.8MB per minute (19.9GB per day).
# MAGIC   * Input data rate can be minimized by appropriate filtering and aggregation.
# MAGIC * is based on list prices for AWS EC2 on-demand rates and for Databrick job compute rates - does not account for special discounts or optimizations like using spot instances
# MAGIC * does not include storage costs or ingestion/ELT costs.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Download Databricks pricing for AWS
# MAGIC %sh
# MAGIC mkdir /dbfs/tmp/rules
# MAGIC cd /dbfs/tmp/rules
# MAGIC pwd
# MAGIC echo "Removing and downloading aws.json"
# MAGIC rm AWS.json
# MAGIC wget https://www.databricks.com/data/pricing/AWS.json
# MAGIC ls -l

# COMMAND ----------

tb="aws"
jsonfile="/tmp/rules/AWS.json"
df = spark.read.format("json").load(jsonfile)
df.createOrReplaceTempView(tb)

# COMMAND ----------

# DBTITLE 1,Runtime model for each ec2 type (empirically constructed)
runtime_model = {
  "i3.xlarge": {
    "udf_sql": {
      "500": [
        50000,
        3.441438913345337,
        400000,
        15.520002603530884
      ],
      "1000": [
        50000,
        4.017544984817505,
        400000,
        22.030455589294434
      ],
      "2000": [
        50000,
        7.723710298538208,
        400000,
        39.17914652824402
      ],
      "4000": [
        50000,
        9.105348587036133,
        400000,
        66.15570950508118
      ]
    },
    "pd_udf_sql": {
      "500": [
        50000,
        2.839707851409912,
        400000,
        13.722443342208862
      ],
      "1000": [
        50000,
        4.2576234340667725,
        400000,
        20.945995092391968
      ],
      "2000": [
        50000,
        7.9649882316589355,
        400000,
        37.47307562828064
      ],
      "4000": [
        50000,
        8.631979942321777,
        400000,
        64.08518719673157
      ]
    }
  },
  "i3.2xlarge": {
    "udf_sql": {
      "500": [
        50000,
        3.441438913345337,
        400000,
        15.520002603530884
      ],
      "1000": [
        50000,
        4.017544984817505,
        400000,
        22.030455589294434
      ],
      "2000": [
        50000,
        7.723710298538208,
        400000,
        39.17914652824402
      ],
      "4000": [
        50000,
        9.105348587036133,
        400000,
        66.15570950508118
      ]
    },
    "pd_udf_sql": {
      "500": [
        50000,
        2.839707851409912,
        400000,
        13.722443342208862
      ],
      "1000": [
        50000,
        4.2576234340667725,
        400000,
        20.945995092391968
      ],
      "2000": [
        50000,
        7.9649882316589355,
        400000,
        37.47307562828064
      ],
      "4000": [
        50000,
        8.631979942321777,
        400000,
        64.08518719673157
      ]
    }
  },
  "m5d.large": {
  "udf_sql": {
    "500": [
      50000,
      3.441438913345337,
      400000,
      15.520002603530884
    ],
    "1000": [
      50000,
      4.017544984817505,
      400000,
      22.030455589294434
    ],
    "2000": [
      50000,
      7.723710298538208,
      400000,
      39.17914652824402
    ],
    "4000": [
      50000,
      9.105348587036133,
      400000,
      66.15570950508118
    ]
  },
  "pd_udf_sql": {
    "500": [
      50000,
      2.839707851409912,
      400000,
      13.722443342208862
    ],
    "1000": [
      50000,
      4.2576234340667725,
      400000,
      20.945995092391968
    ],
    "2000": [
      50000,
      7.9649882316589355,
      400000,
      37.47307562828064
    ],
    "4000": [
      50000,
      8.631979942321777,
      400000,
      64.08518719673157
    ]
  }
}
}



# COMMAND ----------

# DBTITLE 1,Load a portion of the DBU pricing into memory
sql = f"""
select instance, dburate, hourrate
from {tb}
where compute='Jobs Compute' and instance in ('i3.xlarge', 'i3.2xlarge', 'm5d.large') and plan = 'Premium'
"""
df=spark.sql(sql)
display(df)

dbu_rates = {}
for (i, dr, hr) in df.collect():
  dbu_rates[i] = float(dr) * float(hr)
  #print(f"{i} -> {dr} {hr}")

# COMMAND ----------

# DBTITLE 1,Functions for estimating monthly compute cost
# does not include storage costs or ingest+ELT costs
# freq is in minutes, freq==0 denotes streaming
def estimate_monthly_cost(runtime_sec, ec2_type, freq=5):
  if freq==0.0:
    monthly_compute_hrs = 24.0 * 30.0
  else:
    assert runtime_sec < freq * 60
    nbatches_per_month = 24.0 * 30.0 * 60.0 / freq
    monthly_compute_hrs = (runtime_sec / 60.0 / 60.0) * nbatches_per_month
  ec2_rate = {
    "i3.2xlarge": 0.624,
    "i3.xlarge": 0.312,
    "m5d.large": 0.113
  }
  
  assert (ec2_type in ec2_rate)
  assert (ec2_type in dbu_rates)
  aws = monthly_compute_hrs * ec2_rate[ec2_type]
  dbu_cost = monthly_compute_hrs * dbu_rates[ec2_type]
  #print(f"{monthly_compute_hrs} -> {aws}, {dbu_cost}")
  return [ aws, dbu_cost, aws+dbu_cost ]

# side effect: alters metrics list of lists
def add_cost_col(metrics, ec2_type, freq, colidx=4):
  for row in metrics:
    row.extend(estimate_monthly_cost(row[colidx], ec2_type, freq))
  return None

# eqn of a line: y = mx + b
def get_eqn_of_line(x1, y1, x2, y2):
  m = (y2-y1)/(x2-x1)*1.0
  b = y2 - m * x2
  return (m, b)

def get_y(m,b,x):
  return m*x+b

def get_runtime_sec(runtime_model, ec2_type, udf_type, nq, nr):
  (x1, y1, x2, y2) = runtime_model[ec2_type][udf_type][nq]
  (m, b) = get_eqn_of_line(x1, y1, x2, y2)
  return get_y(m,b,nr)

# sanity tests
(m, b) = get_eqn_of_line(50000, 8.6, 400000, 64)
print ( (m,b))
print ("get_y: " + str(get_y(m,b, 100000)))
print ("get_runtime_sec: " + str(get_runtime_sec(runtime_model, "i3.xlarge", "pd_udf_sql", "4000", 100000)))

print(estimate_monthly_cost(16.0, "i3.xlarge", 0))
print(estimate_monthly_cost(16.0, "i3.xlarge", 5))
print(estimate_monthly_cost(8.0, "i3.2xlarge", 5))

test_metrics = [
["case_sql",20,100000,500,15.810449838638306],
["case_sql",20,100000,1000,57.52072048187256]]
add_cost_col(test_metrics, "i3.xlarge", 10)
print(test_metrics)



# COMMAND ----------

# DBTITLE 1,Sample Single-node Compute Cost Estimator
import ipywidgets as widgets
import seaborn as sns 
from ipywidgets import interact

@interact(ec2_type=["i3.xlarge", "i3.2xlarge", "m5d.large"], nqueries=["500", "1000", "2000", "4000"], rows_per_min=[20000, 40000, 80000, 160000])
def plot_costs(ec2_type, nqueries, rows_per_min):
  freq = [5, 15, 30, 60, 24*60]
  cost_metrics = []
  for f in freq:
    # each row is about 692 bytes
    nr = f * rows_per_min
    runtime_sec = get_runtime_sec(runtime_model, ec2_type, "pd_udf_sql", nqueries, nr)
    (aws, dbu, cost) = estimate_monthly_cost(runtime_sec, ec2_type, f)
    cost_metrics.append([f, aws, dbu ])
  print(cost_metrics)
  cost_df = spark.createDataFrame(cost_metrics, schema="freq int, aws double, dbu double")
  pdf = cost_df.toPandas()
  pdf.plot(kind='bar', x="freq", stacked=True, xlabel='Periodicity (minutes)', ylabel='monthly cost (USD)', rot=0)

# COMMAND ----------


