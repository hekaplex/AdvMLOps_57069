# Databricks notebook source
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

# MAGIC %run ./Classroom-Setup

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, when, mean

# Initialize the success flag and status message
success_flag = True
status_message = "SUCCESS"

# COMMAND ----------

try:
    # Step 1: Define the dataset path and read the dataset
    try:
        data_path = f"{DA.paths.datasets.cdc_diabetes}/cdc-diabetes/diabetes_binary_5050split_BRFSS2015.csv"
        raw_data = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(data_path)
        )
        print("Dataset successfully loaded.")
    except Exception as e:
        print(f"Error reading dataset: {e}")
        status_message = "FAILED"
        raise

    # Step 2: Handle missing values
    try:
        columns_to_clean = [col_name for col_name in raw_data.columns if raw_data.select(col_name).schema[0].dataType.typeName() in ['int', 'double']]

        for column in columns_to_clean:
            mean_value = raw_data.select(mean(col(column)).alias("mean")).first()["mean"]
            raw_data = raw_data.fillna({column: mean_value})
        print("Missing values handled successfully.")
    except Exception as e:
        print(f"Error handling missing values: {e}")
        status_message = "FAILED"
        raise

    # Step 3: Add a health risk label for classification
    try:
        cleaned_data = raw_data.withColumn(
            "health_risk_label",
            when(col("Diabetes_binary") == 1, "High Risk").otherwise("Low Risk")
        )
        print("Health risk label added successfully.")
    except Exception as e:
        print(f"Error adding health risk label: {e}")
        status_message = "FAILED"
        raise

    # Step 4: Save the cleaned data to Delta format
    try:
        cleaned_data_path = f"{DA.catalog_name}.{DA.schema_name}.cleaned_diabetes_data"
        cleaned_data.write.format("delta").mode("overwrite").saveAsTable(cleaned_data_path)
        print(f"Cleaned data saved to: {cleaned_data_path}")
    except Exception as e:
        print(f"Error saving cleaned data: {e}")
        status_message = "FAILED"
        raise

except Exception:
    print("Notebook exited: FAILURE")
    success_flag = False
    dbutils.notebook.exit("FAILED")

# Final output if all steps succeed
if success_flag:
    print("Notebook exited: SUCCESS")
    dbutils.notebook.exit("SUCCESS")
else:
    dbutils.notebook.exit("FAILED")
