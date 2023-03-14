# Databricks notebook source
# MAGIC %md
# MAGIC ### Librerias

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.functions import max, col
from pyspark.sql.functions import col, when, concat, md5, lit, coalesce, upper

# COMMAND ----------

# MAGIC %md
# MAGIC ###Fijar catálogo

# COMMAND ----------

spark.sql("USE CATALOG poc_catalog") 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tablas

# COMMAND ----------

M = spark.table("bronze.merchants").alias("M")
MA = spark.table("bronze.merchants_aud").alias("MA")
MI = spark.table("bronze.merchants_info").alias("MI")
FP = spark.table("bronze.fee_plans").alias("FP")
FPC = spark.table("bronze.fee_plan_costs").alias("FPC")
RI = spark.table("bronze.rev_info").alias("RI")
