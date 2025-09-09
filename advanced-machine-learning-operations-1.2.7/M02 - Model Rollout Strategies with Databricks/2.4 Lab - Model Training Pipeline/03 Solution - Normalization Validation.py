# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Notebook - Normalization Validation
# MAGIC
# MAGIC 1. **Purpose of the Notebook**:
# MAGIC    - In this notebook, called **Features Validation**, we will validate the feature table created in the previous notebook.
# MAGIC
# MAGIC 2. **Validation Process**:
# MAGIC    - The feature table is read from the **Feature Store**.
# MAGIC    - The focus is on testing whether **normalization** has been correctly applied to the **normalized column** (e.g., the Age column).
# MAGIC
# MAGIC 3. **Expected Outcome**:
# MAGIC    - If normalization has occurred properly, you will receive a confirmation message indicating that the column has been correctly normalized.

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
normalized_column = dbutils.widgets.get('normalized_column')
silver_table_name = dbutils.widgets.get('silver_table_name')

# COMMAND ----------


from databricks.feature_engineering import FeatureEngineeringClient

# Instantiate the FeatureEngineeringClient
fe = FeatureEngineeringClient()

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

import numpy as np

# Test function to check normalization
def test_column_normalized(df, column):
    if column not in df.columns:
        raise AssertionError(f"Column '{column}' does not exist in the DataFrame.")
    
    mean = np.mean(df[column])
    std = np.std(df[column])
    
    # Allowing a small tolerance for floating-point arithmetic
    tolerance = 1e-4
    assert abs(mean) < tolerance, f"Mean of column '{column}' is not approximately 0. It is {mean}."
    assert abs(std - 1) < tolerance, f"Standard deviation of column '{column}' is not approximately 1. It is {std}."
    print(f"Column '{column}' is properly normalized.")

# COMMAND ----------

# read table from feature store
df2 = fe.read_table(name=f'{silver_table_name}_features').toPandas()

# COMMAND ----------


test_column_normalized(df2, f'{normalized_column}_normalized')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
