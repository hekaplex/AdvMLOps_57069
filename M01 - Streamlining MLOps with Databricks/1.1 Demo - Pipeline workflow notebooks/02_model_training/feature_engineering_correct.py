# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Engineering
# MAGIC This notebook performs advanced feature engineering on the transformed dataset for model training.

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
from pyspark.sql.functions import col, pow

# Initialize the success flag and status message
success_flag = True
feature_status = "SUCCESS"

try:
    # Step 1: Read the transformed data from Delta table
    try:
        transformed_data = spark.table(f"{DA.catalog_name}.{DA.schema_name}.transformed_diabetes_data")
        print("Transformed data successfully loaded.")
    except Exception as e:
        print(f"Error reading transformed data: {e}")
        feature_status = "FAILURE"
        raise

    # Step 2: Display the transformed data
    try:
        display(transformed_data)
    except Exception as e:
        print(f"Error displaying transformed data: {e}")
        feature_status = "FAILURE"
        raise

    # Step 3: CORRECT Perform Feature Engineering
    try:
        # Intentionally using an incorrect column name for demonstration
        engineered_data = transformed_data \
            .withColumn("BMI_squared", pow(col("BMI"), 2)) \
            .withColumn("MentHlth_squared", pow(col("MentHlth"), 2)) \
            .withColumn("BMI_MentHlth_interaction", col("BMI") * col("MentHlth"))
        print("Feature engineering completed successfully.")
    except Exception as e:
        print(f"Error during feature engineering: {e}")
        feature_status = "FAILURE"
        raise
    # Step 4: Check if the dataset meets the required condition
    try:
        required_row_count = 1000  # Minimum number of rows required
        actual_row_count = engineered_data.count()

        if actual_row_count < required_row_count:
            print(f"Insufficient data: {actual_row_count} rows. Stopping pipeline.")
            feature_status = "FAILURE"
            raise Exception("Dataset row count below the required threshold.")
        else:
            print(f"Data check passed: {actual_row_count} rows. Proceeding with the pipeline.")
    except Exception as e:
        print(f"Error during row count check: {e}")
        feature_status = "FAILURE"
        raise

    # Step 5: Drop unnecessary columns
    try:
        columns_to_drop = ["sqrt_PhysHlth", "sqrt_MentHlth"]
        engineered_data = engineered_data.drop(*columns_to_drop)
        print("Unnecessary columns dropped successfully.")
    except Exception as e:
        print(f"Error during column drop: {e}")
        feature_status = "FAILURE"
        raise

    # Step 6: Save the feature-engineered data to Delta format
    try:
        # Define the path to save the data
        feature_data_path = f"{DA.catalog_name}.{DA.schema_name}.feature_engineered_data"
        
        # Save the engineered data to a Delta table
        engineered_data.write.format("delta").mode("overwrite").saveAsTable(feature_data_path)
        print(f"Feature-engineered data saved to: {feature_data_path}")
        
        # Set the task value to indicate success
        dbutils.jobs.taskValues.set(key="feature_engineering_status", value="SUCCESS")
    except Exception as e:
        print(f"Error saving feature-engineered data: {e}")
        feature_status = "FAILURE"
        raise

except Exception:
    # Log the failure status
    print("Notebook exited: FAILURE")
    dbutils.jobs.taskValues.set(key="feature_engineering_status", value="FAILURE")
    dbutils.notebook.exit("FAILURE")

# Final output if all steps succeed
print("Notebook exited: SUCCESS")
dbutils.notebook.exit("SUCCESS")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
