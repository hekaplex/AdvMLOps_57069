# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Evaluation and Testing with Accuracy Check
# MAGIC
# MAGIC This notebook evaluates the performance of a trained model, generates predictions, and validates its accuracy against a defined threshold to determine the pipeline's progression.

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
# MAGIC ### Steps Covered in This Notebook
# MAGIC
# MAGIC 1. **Model Evaluation and Accuracy Check**:
# MAGIC    - Load feature-engineered data and assemble features.
# MAGIC    - Retrieve the trained model from Unity Catalog, generate predictions, and evaluate accuracy using the Area Under ROC (AUC) metric. 
# MAGIC    - Compare the accuracy against a user-defined threshold to determine if the pipeline should proceed.
# MAGIC
# MAGIC 2. **Metrics Logging and Validation**:
# MAGIC    - Save predictions to Delta tables and log evaluation metrics (accuracy and threshold) to MLflow for tracking.
# MAGIC    - Exit the notebook with a success or failure status based on the accuracy validation.
# MAGIC

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from mlflow.tracking import MlflowClient
import mlflow

# Widgets for environment-specific configurations
dbutils.widgets.text("accuracy_threshold", "0.6", "Accuracy Threshold")
dbutils.widgets.text("model_name", "telco_churn_model", "Model Name")

# Get widget values
accuracy_threshold = float(dbutils.widgets.get("accuracy_threshold"))
model_name = dbutils.widgets.get("model_name") if dbutils.widgets.get("model_name") == 'telco_churn_model' else 'telco_churn_model_dev'

# Define paths for data and predictions
feature_engineered_data_path = f"{DA.catalog_name}.{DA.schema_name}.feature_engineered_telco_data"
prediction_table_path = f"{DA.catalog_name}.{DA.schema_name}.prediction_table"

# Step 1: Load Feature-Engineered Data
print("Step 1: Loading feature-engineered data...")
try:
    feature_data = spark.table(feature_engineered_data_path)
    if feature_data.count() == 0:
        raise ValueError("Feature-engineered data is empty. Ensure data transformation and feature engineering steps are complete.")
    print(f"Feature data contains {feature_data.count()} rows.")
    display(feature_data)
except Exception as e:
    raise ValueError(f"Failed to load feature-engineered data: {e}")

# Step 2: Assemble Features
print("Step 2: Assembling features...")
assembler = VectorAssembler(
    inputCols=["tenure", "MonthlyCharges", "log_tenure", "sqrt_MonthlyCharges"],
    outputCol="features"
)
try:
    feature_data = assembler.transform(feature_data)
    print("Features successfully assembled.")
    display(feature_data)
except Exception as e:
    raise ValueError(f"Failed to assemble features: {e}")

# Step 3: Load the Model from Unity Catalog
print("Step 3: Loading the model...")
mlflow.set_registry_uri("databricks-uc")

# Define full Unity Catalog model path
full_model_name = f"{DA.catalog_name}.{DA.schema_name}.{model_name}"
client = MlflowClient()

try:
    # Retrieve all model versions from Unity Catalog
    all_versions = client.search_model_versions(f"name='{full_model_name}'")

    # Check if "Champion" alias exists
    champion_version = None
    for mv in all_versions:
        version_details = client.get_model_version(full_model_name, mv.version)
        if "Champion" in version_details.aliases:
            champion_version = mv.version
            break

    # If no Champion alias exists, assign it to the latest version
    if champion_version is None:
        latest_version = max(int(mv.version) for mv in all_versions)
        client.set_registered_model_alias(full_model_name, "Champion", version=latest_version)
        champion_version = latest_version
        print(f"Alias 'Champion' assigned to version {latest_version}.")

    # Load model from Unity Catalog using the alias
    model_uri = f"models:/{full_model_name}@Champion"
    print(f"Attempting to load model from: {model_uri}")
    model = mlflow.spark.load_model(model_uri)
    print(f"Model loaded successfully from: {model_uri}")

except mlflow.exceptions.MlflowException as e:
    raise ValueError(f"Failed to load the model from Unity Catalog. Ensure the model '{model_name}' is registered and available. Error: {e}")
except Exception as e:
    raise ValueError(f"Unexpected error while loading the model: {e}")

# Step 4: Generate Predictions
print("Step 4: Generating predictions...")
try:
    predictions = model.transform(feature_data)
    if predictions.count() == 0:
        raise ValueError("No predictions generated. Verify the model and input data compatibility.")
    print(f"Predictions generated successfully: {predictions.count()} rows.")
    display(predictions)

    # Enable schema migration for Delta table
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    # Save predictions to a Delta table with schema migration
    predictions.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(prediction_table_path)
    print(f"Predictions saved to: {prediction_table_path}")
except Exception as e:
    raise ValueError(f"Failed to generate predictions: {e}")

# Step 5: Evaluate Model Accuracy
print("Step 5: Evaluating model accuracy...")
evaluator = BinaryClassificationEvaluator(
    labelCol="Churn", rawPredictionCol="prediction", metricName="areaUnderROC"
)
try:
    accuracy = evaluator.evaluate(predictions)
    print(f"Model accuracy (Area Under ROC): {accuracy}")
except Exception as e:
    raise ValueError(f"Failed to evaluate the model: {e}")

# Step 6: Compare Accuracy with Threshold
print("Step 6: Validating accuracy against the threshold...")
if accuracy >= accuracy_threshold:
    print(f"Model accuracy {accuracy} meets the threshold {accuracy_threshold}. Proceeding with the pipeline.")
    dbutils.notebook.exit("SUCCESS")
else:
    print(f"Model accuracy {accuracy} is below the threshold {accuracy_threshold}. Stopping the pipeline.")
    dbutils.notebook.exit("FAILURE")

# Step 7: Log Evaluation Metrics to MLflow
print("Step 7: Logging evaluation metrics to MLflow...")
experiment_name = f"/Shared/{DA.username}_telco_churn_evaluation"
mlflow.set_experiment(experiment_name)

try:
    with mlflow.start_run():
        mlflow.log_param("accuracy_threshold", accuracy_threshold)
        mlflow.log_metric("accuracy", accuracy)
    print("Evaluation metrics logged to MLflow successfully.")
except Exception as e:
    raise ValueError(f"Failed to log evaluation metrics to MLflow: {e}")

print("Notebook execution completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
