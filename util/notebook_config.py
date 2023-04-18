# Databricks notebook source
import os
import json
import re

cfg={}
cfg["useremail"] = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
cfg["username"] = cfg["useremail"].split('@')[0]
cfg["username_sql_compatible"] = re.sub('\W', '_', cfg["username"])
cfg["db"] = f"rules_{cfg['username_sql_compatible']}"
cfg["data_path"] = f"/tmp/{cfg['username_sql_compatible']}/rules/"
cfg["download_path"] = "/tmp/rules"
cfg["folders"] = ["2021-10", "2021-12"]
cfg["tables"] = ["conn",
"dce_rpc",
"dhcp",
"dns",
"dpd",
"files",
"http",
"kerberos",
"ntlm",
"ocsp",
"pe",
"smb_files",
"smb_mapping",
"smtp",
"ssl",
"weird",
"x509"]

if "getParam" not in vars():
  def getParam(param):
    assert param in cfg
    return cfg[param]

print(json.dumps(cfg, indent=2))
spark.sql(f"use {getParam('db')}")

# COMMAND ----------

# DBTITLE 1,mlflow settings
#import mlflow
#model_name = "iot_anomaly_detection_xgboost"
#username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
#mlflow.set_experiment('/Users/{}/iot_anomaly_detection'.format(username))

# COMMAND ----------

# DBTITLE 1,Streaming checkpoint location
checkpoint_path = "/dbfs/tmp/rules_engines/checkpoints"
