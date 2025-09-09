# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Notebook - Silver to Feature Store
# MAGIC
# MAGIC 1. **Widgets at the Top**:
# MAGIC    - In this notebook, you will find several parameterized widgets:
# MAGIC      - **catalog**
# MAGIC      - **column**
# MAGIC      - **primary_key**
# MAGIC      - **schema**
# MAGIC      - **silver_table_name**
# MAGIC      - **target_column**
# MAGIC
# MAGIC 2. **Purpose of Parameterization**:
# MAGIC    - These widgets allow you to configure parameters dynamically when setting up workflows.
# MAGIC    - Instead of modifying hard-coded values in the notebook, you can edit the parameters directly in the Databricks Workflows UI.
# MAGIC
# MAGIC 3. **Notebook Functionality**:
# MAGIC    - This notebook focuses on **feature engineering**.
# MAGIC    - Specifically, it normalizes the **Age** column and generates a feature table.
# MAGIC    - The resulting feature table is stored in the **Feature Store** for use in downstream tasks like model training or evaluation.

# COMMAND ----------

# MAGIC %md
# MAGIC Read in silver-layer data.

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

silver_table_name = dbutils.widgets.get("silver_table_name")
df = spark.read.format('delta').table(silver_table_name).select('id', 'Diabetes_binary', 'HighBP', 'BMI', 'Smoker', 'Stroke', 'HeartDiseaseorAttack', 'Age'). toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC Perform feature engineering - normalize your column of choice.

# COMMAND ----------


import pandas as pd
import numpy as np

from databricks.feature_engineering import FeatureEngineeringClient

## Instantiate the FeatureEngineeringClient
fe = FeatureEngineeringClient()

## Normalize the Age column and store it as Age_normalized

column = dbutils.widgets.get("column")
target_column = dbutils.widgets.get("target_column")

df[f'{column}_normalized'] = (df[column] - df[column].mean()) / df[column].std()


df = df.drop(target_column, axis=1)
df = df.drop(column, axis=1)
normalized_df = spark.createDataFrame(df)

primary_key = dbutils.widgets.get("primary_key")

## Set the feature table name for storage in UC
feature_table_name = f'{catalog}.{schema}.{silver_table_name}_features'

## print(f"The name of the feature table: {feature_table_name}\n\n")

spark.sql(f'drop table if exists {feature_table_name}')

## Create the feature table
fe.create_table(
    name = feature_table_name,
    primary_keys = primary_key,
    df = normalized_df, 
    description="{schema} quality features", 
    tags = {"source": "silver", "format": "delta"}
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
