# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Model Training
# MAGIC This notebook trains a machine learning model and registers it in MLflow.

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
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import mlflow
import mlflow.sklearn

# COMMAND ----------

# Widgets for environment-specific configurations
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment Name")
# Define the path to read the feature-engineered data
feature_data_path = f"{DA.catalog_name}.{DA.schema_name}.feature_engineered_data"

# Read the feature-engineered DataFrame from Delta format
feature_data = spark.read.format("delta").table(feature_data_path)

env = dbutils.widgets.get("env")

# Display the feature-engineered data
display(feature_data)

# COMMAND ----------

# Assemble features for ML model
assembler = VectorAssembler(
    inputCols=[
        "HighBP", "BMI", "Smoker", "PhysActivity", "Fruits", 
        "Veggies", "MentHlth_squared", "BMI_squared", 
        "BMI_MentHlth_interaction", "Age"
    ],
    outputCol="features"
)
feature_data = assembler.transform(feature_data)

# Split the dataset into training and test sets
train_data, test_data = feature_data.randomSplit([0.8, 0.2], seed=42)

# COMMAND ----------

# Train a Random Forest model
rf = RandomForestRegressor(featuresCol="features", labelCol="Diabetes_binary")
model = rf.fit(train_data)

# Evaluate the model
evaluator = RegressionEvaluator(labelCol="Diabetes_binary", predictionCol="prediction", metricName="rmse")
predictions = model.transform(test_data)
rmse = evaluator.evaluate(predictions)

print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

# COMMAND ----------

# Set up MLflow experiment tracking
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient

experiment_name = f"/Shared/{DA.username}_adv_mlops_demo_diabetes"
mlflow.set_experiment(experiment_name)

# Infer model signature
signature = infer_signature(
    train_data.select("features").toPandas(),  # Inputs (features)
    model.transform(train_data).select("prediction").toPandas()  # Outputs (predictions)
)

catalog_name = "main"
schema_name = "public"
model_base_name = f"diabetes_model_{env}"
full_model_name = f"{DA.catalog_name}.{DA.schema_name}.{model_base_name}"

with mlflow.start_run():
    mlflow.log_param("environment", env)
    mlflow.log_metric("rmse", rmse)
    mlflow.spark.log_model(
        model,
        artifact_path="model",
        registered_model_name=full_model_name,  # Add model signature
        signature=signature
    )

# Ensure the model is registered and available in MLflow
client = MlflowClient()
model_version_infos = client.search_model_versions(f"name = '{full_model_name}'")
if model_version_infos:
    latest_version = max([int(info.version) for info in model_version_infos])
    model_uri = f"models:/{full_model_name}/{latest_version}"
    loaded_model = mlflow.spark.load_model(model_uri)
    print(f"Model registered and loaded for environment: {env}")
else:
    print(f"Model {full_model_name} is not registered.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
