# Databricks notebook source
from jobs import get_job_url

# COMMAND ----------

get_job_url(
    host="<host-url>",
    access_token="<access-token>",
    job_name="<workflow-name>",
    job_description="<workflow-description>",
    cluster_compute_config_path="<cluster-compute-config-path>",
    warehouse_compute_config_path="<warehouse-config-path>",
)
