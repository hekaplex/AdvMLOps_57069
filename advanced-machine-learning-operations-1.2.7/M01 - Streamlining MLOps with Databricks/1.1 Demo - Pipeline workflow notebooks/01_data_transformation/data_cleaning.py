# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning
# MAGIC This notebook focuses on cleaning the raw dataset and preparing it for transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need a classic cluster running one of the following Databricks runtime(s): **16.3.x-cpu-ml-scala2.12**. **Do NOT use serverless compute to run this notebook**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run ../../../Includes/Classroom-Setup

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, when, mean

# Define the dataset path
data_path = f"{DA.paths.datasets.cdc_diabetes}/cdc-diabetes/diabetes_binary_5050split_BRFSS2015.csv"

# Read the dataset
raw_data = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(data_path)
)

# COMMAND ----------

# Handle missing values
# Replace missing values in numeric columns with their mean
columns_to_clean = [col_name for col_name in raw_data.columns if raw_data.select(col_name).schema[0].dataType.typeName() in ['int', 'double']]

for column in columns_to_clean:
    mean_value = raw_data.select(mean(col(column)).alias("mean")).first()["mean"]
    raw_data = raw_data.fillna({column: mean_value})

# Display the cleaned dataset
display(raw_data)

# COMMAND ----------

# Add a health risk label for classification (example transformation)
cleaned_data = raw_data.withColumn(
    "health_risk_label",
    when(col("Diabetes_binary") == 1, "High Risk").otherwise("Low Risk")
)

# Display the transformed dataset
display(cleaned_data)

# COMMAND ----------

# Define the path to save the cleaned data
cleaned_data_path = f"{DA.catalog_name}.{DA.schema_name}.cleaned_diabetes_data"

# Write the cleaned DataFrame to Delta format
cleaned_data.write.format("delta").mode("overwrite").saveAsTable(cleaned_data_path)

print(f"Cleaned data saved to: {cleaned_data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
