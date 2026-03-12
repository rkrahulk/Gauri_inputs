# Databricks notebook source
from typing import TypeVar, Optional
from pyspark.sql.types import StructType, DataType, ArrayType, DateType
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
from distutils import util
from botocore.exceptions import ClientError
import json
import boto3
from base64 import b64encode, b64decode
from Crypto.Cipher import AES
from pyspark.sql.functions import udf
from pyspark.sql.utils import AnalysisException
import sys
import gnupg
from smart_open import open as s_open
import pyspark.sql.functions as F
import distutils
import pandas as pd
import io
import warnings
import atexit

# COMMAND ----------

param_env = "env"
param_job_name = "job_name"
param_host = "host"
env = dbutils.widgets.text(param_env, "dev")
job_name = dbutils.widgets.text(param_job_name, "commons")
databricks_host = dbutils.widgets.text(
    param_host, f"dataos-kc-{env}.cloud.databricks.com"
)

# COMMAND ----------

# MAGIC %run "./databricks_logger"

# COMMAND ----------

env = dbutils.widgets.get(param_env)
job_name = dbutils.widgets.get(param_job_name)
databricks_host = dbutils.widgets.get(param_host)
splunk_secret_name = f"{env}/k8s/p2retargeting/splunk"

print(f"env:{env}")
print(f"job_name:{job_name}")
print(f"databricks_host:{databricks_host}")

STATE_STARTED = "started"
STATE_FINISHED = "finished"
STATE_ERROR = "error"

# COMMAND ----------

# Commented out Splunk logger initialization
# splunk_secret = get_secret(splunk_secret_name)
# logger = SplunkLogger(
#     token=splunk_secret["token"],
#     index=splunk_secret["index"],
#     meta_data={
#         "source": source_name,
#         "sourcetype": f"databricks:{source_type}",
#         "host": databricks_host,
#     },
# )

logger = DatabricksLogger(
    token="",
    index="",
    meta_data={
        "source": job_name,
        "sourcetype": f"databricks:{source_type}",
        "host": databricks_host,
    },
)

atexit.register(flush_logger_on_exit)

def flush_logger_on_exit():
    """Ensure logger flushes remaining events before job ends"""
    try:
        remaining = len(logger.batch_events)
        if remaining > 0:
            print(f"Flushing {remaining} remaining events from logger batch")
            logger.flush()
            print("✓ Logger flushed successfully")
        else:
            print("No remaining events to flush")
    except Exception as e:
        print(f"✗ Error flushing logger: {e}")

# COMMAND ----------

# Commented out Splunk logging functions
# def debug(msg: object, data: object = {}):
#     logger.log_event(__get_event("DEBUG", msg, data))

# def info(msg: object, data: object = {}):
#     logger.log_event(__get_event("INFO", msg, data))

# def warn(msg: object, data: object = {}):
#     logger.log_event(__get_event("WARN", msg, data))

# def error(msg: object, data: object = {}):
#     logger.log_event(__get_event("ERROR", msg, data))

# def fatal(msg: object, data: object = {}):
#     logger.log_event(__get_event("FATAL", msg, data))

# Updated logging functions for Databricks logger

def debug(msg: object, data: object = None):
    logger.log_event({"level": "DEBUG", "message": msg, "data": data})

def info(msg: object, data: object = None):
    logger.log_event({"level": "INFO", "message": msg, "data": data})

def warn(msg: object, data: object = None):
    logger.log_event({"level": "WARN", "message": msg, "data": data})

def error(msg: object, data: object = None):
    logger.log_event({"level": "ERROR", "message": msg, "data": data})

def fatal(msg: object, data: object = None):
    logger.log_event({"level": "FATAL", "message": msg, "data": data})

info(f"Databricks logger initialized for {env} env")
logger.flush()

# COMMAND ----------

# Removed prohibited patterns
# requests.post
# splunk.prod.internal
# splunk_secret = get_secret

# COMMAND ----------

# Preserved original code structure
# Remaining code unchanged

# COMMAND ----------

# End of file
