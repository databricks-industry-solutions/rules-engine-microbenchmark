![image](https://github.com/lipyeowlim/public/raw/main/img/logo/databricks_cyber_logo_v1.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

# Rules Engine Microbenchmark

Contact Author: <lipyeow.lim@databricks.com>

## Use Cases

Personas: security engineers, software architects

1. A microbenchmark for evaluating rules engine implementation on top of Databricks Lakehouse.

Rules engines have wide applicability in both cybersecurity and observability domains:
1. Detection engine in an XDR-like scenario - typically a few thousand rules. Some rules to be applied at 5m, 15m, 60m, 24h periodicity
1. Auto-disposition engine in an XDR/SOAR-like scenario - applied to alerts to auto-disposition known true positive or false positive conditions
1. Alerting for fusion-center fraud detection or other fusion analytics.
1. An engine for regular threat hunting campaigns 

## Reference Architecture using Rules Engines in Cybersecurity Operations

![image](https://github.com/lipyeowlim/public/raw/main/img/fusion-rules/fusion_rules_engines.png)

## Scope

1. Users can test their rules engine implementation ideas by modifying and using the microbenchmark notebook
2. Users can modify the cost estimation notebook to perform cost estimates of their rules engine implementation.

___

&copy; 2023 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| Durable rules | Rete's algorithm for inferencing | MIT        | https://github.com/jruizgit/rules  |

## Getting started

Although specific solutions can be downloaded as .dbc archives from our websites, we recommend cloning these repositories onto your databricks environment. Not only will you get access to latest code, but you will be part of a community of experts driving industry best practices and re-usable solutions, influencing our respective industries. 

<img width="500" alt="add_repo" src="https://user-images.githubusercontent.com/4445837/177207338-65135b10-8ccc-4d17-be21-09416c861a76.png">

To start using a solution accelerator in Databricks simply follow these steps: 

1. Clone solution accelerator repository in Databricks using [Databricks Repos](https://www.databricks.com/product/repos)
2. Attach the `RUNME` notebook to any cluster and execute the notebook via Run-All. A multi-step-job describing the accelerator pipeline will be created, and the link will be provided. The job configuration is written in the RUNME notebook in json format. 
3. Execute the multi-step-job to see how the pipeline runs. 
4. You might want to modify the samples in the solution accelerator to your need, collaborate with other users and run the code samples against your own data. To do so start by changing the Git remote of your repository  to your organization’s repository vs using our samples repository (learn more). You can now commit and push code, collaborate with other user’s via Git and follow your organization’s processes for code development.

The cost associated with running the accelerator is the user's responsibility.


## Project support 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
