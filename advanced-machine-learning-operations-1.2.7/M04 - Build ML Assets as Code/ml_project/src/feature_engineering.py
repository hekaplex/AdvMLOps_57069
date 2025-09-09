# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Engineering
# MAGIC This notebook performs advanced feature engineering on the transformed dataset for model training.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run ./Classroom-Setup

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
