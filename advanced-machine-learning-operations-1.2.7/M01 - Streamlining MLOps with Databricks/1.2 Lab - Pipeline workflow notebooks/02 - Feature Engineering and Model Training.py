# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Feature Engineering and Model Training
# MAGIC This notebook performs feature engineering on the Telco dataset and trains a machine learning model to predict customer churn.

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
# MAGIC ##Feature Engineering and Model Training
# MAGIC
# MAGIC **Steps Covered:**
# MAGIC - Feature Engineering
# MAGIC - Model Training and Evaluation
# MAGIC - Conditional Execution Logic for Demonstration

# COMMAND ----------

# Import necessary libraries
from pyspark.sql.functions import col, log, sqrt, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from mlflow.models.signature import infer_signature
import mlflow
import mlflow.spark

# Initialize the success flag and status message
success_flag = True
task_status = "SUCCESS"

try:
    # Step 1: Read the Transformed Data
    print("Step 1: Reading transformed data...")
    transformed_data_path = f"{DA.catalog_name}.{DA.schema_name}.transformed_telco_data"
    try:
        transformed_data = spark.table(transformed_data_path)
        print(f"Transformed data successfully loaded. Rows: {transformed_data.count()}")
        display(transformed_data)
    except Exception as e:
        print(f"Error reading transformed data: {e}")
        task_status = "FAILURE"
        raise

    # Step 2: Perform Feature Engineering
    print("Step 2: Performing feature engineering...")
    try:
        engineered_data = transformed_data \
            .withColumn("log_tenure", log(col("tenure") + 1)) \
            .withColumn("sqrt_MonthlyCharges", sqrt(col("MonthlyCharges") + 1)) \
            .withColumn("log_TotalCharges", log(col("TotalCharges") + 1)) \
            .withColumn("is_senior", when(col("SeniorCitizen") == 1, "Yes").otherwise("No")) \
            .withColumn("Churn", when(col("Churn") == "Yes", 1).otherwise(0).cast("int")) \
            .withColumn("error_column", col("NonExistentColumn"))  # Deliberate error
        print("Feature engineering completed successfully.")
        display(engineered_data)
    except Exception as e:
        print(f"Error during feature engineering: {e}")
        task_status = "FAILURE"
        raise

    # Step 3: Save the Feature-Engineered Data
    print("Step 3: Saving feature-engineered data...")
    feature_engineered_data_path = f"{DA.catalog_name}.{DA.schema_name}.feature_engineered_telco_data"
    try:
        engineered_data.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(feature_engineered_data_path)
        print(f"Feature-engineered data saved to: {feature_engineered_data_path}")
    except Exception as e:
        print(f"Error saving feature-engineered data: {e}")
        task_status = "FAILURE"
        raise

    # Step 4: Train the Model
    print("Step 4: Training the model...")
    try:
        # Assemble features for ML model
        assembler = VectorAssembler(
            inputCols=["tenure", "MonthlyCharges", "log_tenure", "sqrt_MonthlyCharges"],
            outputCol="features"
        )
        feature_data = assembler.transform(engineered_data)

        # Split the dataset into training and testing sets
        train_data, test_data = feature_data.randomSplit([0.8, 0.2], seed=42)

        # Train a Random Forest classifier
        rf = RandomForestClassifier(featuresCol="features", labelCol="Churn")
        model = rf.fit(train_data)

        # Evaluate the model
        evaluator = MulticlassClassificationEvaluator(labelCol="Churn", predictionCol="prediction", metricName="accuracy")
        predictions = model.transform(test_data)
        accuracy = evaluator.evaluate(predictions)

        print(f"Model accuracy: {accuracy}")
        display(predictions)
    except Exception as e:
        print(f"Error training or evaluating the model: {e}")
        task_status = "FAILURE"
        raise

    # Step 5: Log the Model in MLflow with Signature
    print("Step 5: Logging the model in MLflow...")
    experiment_name = f"/Shared/{DA.username}_telco_churn"
    mlflow.set_experiment(experiment_name)

    try:
        # Infer the signature from the input and output data
        input_sample = train_data.select("features").toPandas()
        output_sample = predictions.select("prediction").toPandas()
        signature = infer_signature(input_sample, output_sample)

        with mlflow.start_run():
            mlflow.log_metric("accuracy", accuracy)
            mlflow.spark.log_model(
                model, 
                artifact_path="model", 
                registered_model_name=f"{DA.catalog_name}.{DA.schema_name}.telco_churn_model",
                signature=signature
            )
        print("Model logged in MLflow successfully.")
    except Exception as e:
        print(f"Error logging the model in MLflow: {e}")
        task_status = "FAILURE"
        raise

except Exception:
    print("Notebook exited: FAILURE")
    dbutils.jobs.taskValues.set(key="feature_engineering_status", value="FAILURE")
    success_flag = False

# Final output
if success_flag:
    print("Notebook exited: SUCCESS")
    dbutils.jobs.taskValues.set(key="feature_engineering_status", value="SUCCESS")
    dbutils.notebook.exit("SUCCESS")
else:
    print("Notebook exited: FAILURE")
    dbutils.notebook.exit("FAILURE")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
