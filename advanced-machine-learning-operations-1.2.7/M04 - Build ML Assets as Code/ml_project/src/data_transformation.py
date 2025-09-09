# Databricks notebook source
# MAGIC %md
# MAGIC # Data Transformation
# MAGIC This notebook performs feature engineering on the cleaned dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run ./Classroom-Setup

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, log, sqrt

# Initialize success flag and status message
success_flag = True
status_message = "SUCCESS"

# COMMAND ----------

try:
    # Step 1: Read the cleaned data from Delta format
    try:
        cleaned_data = spark.table(f"{DA.catalog_name}.{DA.schema_name}.cleaned_diabetes_data")
        print("Cleaned data successfully loaded.")
    except Exception as e:
        print(f"Error reading cleaned data: {e}")
        status_message = "FAILED"
        raise

    # Step 2: Apply feature engineering transformations
    try:
        transformed_data = cleaned_data \
            .withColumn("log_BMI", log(col("BMI") + 1)) \
            .withColumn("sqrt_PhysHlth", sqrt(col("PhysHlth") + 1)) \
            .withColumn("log_Age", log(col("Age") + 1)) \
            .withColumn("sqrt_MentHlth", sqrt(col("MentHlth") + 1))
        print("Feature engineering transformations applied successfully.")
    except Exception as e:
        print(f"Error during feature engineering: {e}")
        status_message = "FAILED"
        raise

    # Step 3: Drop unnecessary columns
    try:
        columns_to_drop = ["HighChol", "HvyAlcoholConsump"]  # Example columns to drop; adjust as needed
        transformed_data = transformed_data.drop(*columns_to_drop)
        print("Unnecessary columns dropped successfully.")
    except Exception as e:
        print(f"Error during column drop: {e}")
        status_message = "FAILED"
        raise

    # Step 4: Save the transformed data to Delta format
    try:
        transformed_data_path = f"{DA.catalog_name}.{DA.schema_name}.transformed_diabetes_data"
        transformed_data.write.format("delta").mode("overwrite").saveAsTable(transformed_data_path)
        print(f"Transformed data saved to: {transformed_data_path}")
    except Exception as e:
        print(f"Error saving transformed data: {e}")
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
