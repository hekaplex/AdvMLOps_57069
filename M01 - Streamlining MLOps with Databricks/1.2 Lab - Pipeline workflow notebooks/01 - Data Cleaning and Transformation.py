# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Cleaning and Transformation
# MAGIC This notebook handles the cleaning and transformation of the Telco customer churn dataset. It ensures the dataset is ready for further analysis and modeling.

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
# MAGIC Before starting the lab, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run ../../Includes/Classroom-Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ###Steps Covered:
# MAGIC - Clean the dataset by handling missing values and outliers.
# MAGIC - Perform feature engineering for additional insights and prepare the data for machine learning.

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, when, mean

# Define the dataset path
data_path = f"{DA.paths.datasets.telco}/telco/telco-customer-churn-noisy.csv"

# Read the dataset
raw_data = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(data_path)
)

# COMMAND ----------

# Replace missing values in numeric columns with their mean
columns_to_clean = [col_name for col_name in raw_data.columns if raw_data.select(col_name).schema[0].dataType.typeName() in ['double']]
for column in columns_to_clean:
    mean_value = raw_data.select(mean(col(column)).alias("mean")).first()["mean"]
    raw_data = raw_data.fillna({column: mean_value})

# Fill missing string values with "Unknown"
string_columns = [col_name for col_name in raw_data.columns if raw_data.select(col_name).schema[0].dataType.typeName() == 'string']
for column in string_columns:
    raw_data = raw_data.fillna({column: "Unknown"})

# COMMAND ----------

# Add a risk label for churn
cleaned_data = raw_data.withColumn(
    "churn_risk_label",
    when(col("Churn") == "Yes", "High Risk").otherwise("Low Risk")
)
# Display cleaned data
display(cleaned_data)

# COMMAND ----------

#Save the cleaned data
cleaned_data_path = f"{DA.catalog_name}.{DA.schema_name}.cleaned_telco_data"
cleaned_data.write.format("delta").mode("overwrite").saveAsTable(cleaned_data_path)
print(f"Cleaned data saved to: {cleaned_data_path}")

# COMMAND ----------

from pyspark.sql.functions import col, log, sqrt, when

# Feature engineering
transformed_data = cleaned_data \
    .withColumn("log_tenure", log(col("tenure") + 1)) \
    .withColumn("sqrt_MonthlyCharges", sqrt(col("MonthlyCharges") + 1)) \
    .withColumn("log_TotalCharges", log(col("TotalCharges") + 1)) \
    .withColumn("is_senior", when(col("SeniorCitizen") == 1, "Yes").otherwise("No"))

# Drop unnecessary columns
columns_to_drop = ["customerID", "PhoneService"]  # Example columns to drop
transformed_data = transformed_data.drop(*columns_to_drop)

# Display transformed data
display(transformed_data)

# Step 6: Save the transformed data
transformed_data_path = f"{DA.catalog_name}.{DA.schema_name}.transformed_telco_data"
transformed_data.write.format("delta").mode("overwrite").saveAsTable(transformed_data_path)
print(f"Transformed data saved to: {transformed_data_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
