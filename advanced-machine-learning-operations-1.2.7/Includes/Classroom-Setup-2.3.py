# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

import numpy as np
np.set_printoptions(precision=2)

import logging
logging.getLogger("tensorflow").setLevel(logging.ERROR)

# COMMAND ----------

@DBAcademyHelper.add_init
def create_diabetes_table_from_marketplace(self):

    import pandas as pd
    from pyspark.sql.functions import lit, col, monotonically_increasing_id
    import mlflow
    
    spark.sql(f"USE CATALOG {DA.catalog_name}")
    spark.sql(f"USE SCHEMA {DA.schema_name}")
    spark.sql(f"DROP TABLE IF EXISTS diabetes")

   
    shared_volume_name = 'cdc-diabetes'
    csv_name = 'diabetes_binary_5050split_BRFSS2015'

    # # Load in the dataset we wish to work on
    data_path = f"{DA.paths.datasets.cdc_diabetes}/{shared_volume_name}/{csv_name}.csv"

    diabetes_df = spark.read.csv(
        data_path, header="true", inferSchema="true", multiLine="true", escape='"'
    )

    # Add a unique ID column
    diabetes_df = diabetes_df.withColumn("id", monotonically_increasing_id())

    binary_cols = ['Diabetes_binary', 'HighBP', 'BMI', 'Smoker', 'Stroke', 'HeartDiseaseorAttack']
    double_cols = ['Age']
    long_cols = ['id']

    # Convert all columns to double type
    for column in double_cols:
        diabetes_df = diabetes_df.withColumn(column, col(column).cast("double"))

    # Convert all columns to integer type
    for column in binary_cols:
        diabetes_df = diabetes_df.withColumn(column, col(column).cast("integer"))

    # Convert all columns to long type
    for column in long_cols:
        diabetes_df = diabetes_df.withColumn(column, col(column).cast("long"))

    # Add in a row where Heart disease is -1
    diabetes_df = diabetes_df.withColumn("HeartDiseaseorAttack", lit(-1))


    diabetes_df.write.format("delta").saveAsTable("diabetes")

# COMMAND ----------

class payload():
    def __init__(self, data):
        self.data = data
    def as_dict(self):
        return self.data

# COMMAND ----------

# Initialize DBAcademyHelper
DA = DBAcademyHelper() 
DA.init()
