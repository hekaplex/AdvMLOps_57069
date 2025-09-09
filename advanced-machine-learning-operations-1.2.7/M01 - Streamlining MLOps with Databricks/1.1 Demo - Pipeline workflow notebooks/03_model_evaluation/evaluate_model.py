# Databricks notebook source
# MAGIC %md
# MAGIC # Model Evaluation and Testing
# MAGIC This notebook evaluates the performance of the trained model, validates predictions, and includes deliberate errors for troubleshooting exercises.

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

# Widgets for environment-specific configurations
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment Name")
dbutils.widgets.text("model_name", "diabetes_model_dev", "Model Name")

# Get environment and model name from widgets
env = dbutils.widgets.get("env")
model_name = dbutils.widgets.get("model_name")

# COMMAND ----------

# Define the catalog, schema, and table paths
feature_data_path = f"{DA.catalog_name}.{DA.schema_name}.feature_engineered_data"
full_model_name = f"{DA.catalog_name}.{DA.schema_name}.{model_name}"

# Load feature-engineered data
try:
    feature_data = spark.table(feature_data_path)
    if feature_data.count() == 0:
        raise ValueError("The feature-engineered dataset is empty. Ensure the data transformation step completed successfully.")
    else:
        print(f"Feature data contains {feature_data.count()} rows.")
        feature_data.printSchema()
        display(feature_data)
except Exception as e:
    raise ValueError(f"Failed to load feature-engineered data: {e}")

# COMMAND ----------

# Ensure required columns are present
required_columns = [
    "HighBP", "BMI", "Smoker", "PhysActivity", "Fruits", 
    "Veggies", "MentHlth_squared", "BMI_squared", 
    "BMI_MentHlth_interaction", "Age"
]

missing_columns = [col for col in required_columns if col not in feature_data.columns]
if missing_columns:
    raise ValueError(f"Missing columns in feature_data: {missing_columns}")

# COMMAND ----------

# Create feature vector
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=required_columns,
    outputCol="features",
    handleInvalid="skip"
)

try:
    feature_data = assembler.transform(feature_data)
    if feature_data.count() == 0:
        raise ValueError("The feature_data DataFrame is empty after vector assembly. Check the input data and transformation logic.")
    print(f"Feature vector successfully created with {feature_data.count()} rows.")
    display(feature_data)
except Exception as e:
    raise ValueError(f"Vector assembly failed: {e}")

# COMMAND ----------

# Verify model compatibility
from mlflow.tracking import MlflowClient
import mlflow

# Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

try:
    client = MlflowClient()
    model_version = 1  # Replace with the actual version number or use a custom alias
    model_uri = f"models:/{full_model_name}/{model_version}"
    print(f"Attempting to load model from Unity Catalog URI: {model_uri}")
    model = mlflow.spark.load_model(model_uri)
    print(f"Loaded model from: {model_uri}")
except mlflow.exceptions.MlflowException as e:
    raise ValueError(f"Failed to load the model from Unity Catalog. Ensure the model '{full_model_name}' is registered and available in Unity Catalog. Error: {e}")
except Exception as e:
    raise ValueError(f"An unexpected error occurred while loading the model from Unity Catalog: {e}")

# COMMAND ----------

# Generate predictions
try:
    predictions = model.transform(feature_data)
    if predictions.count() == 0:
        raise ValueError("The model failed to generate any predictions. Check the input features and model compatibility.")
    print(f"Predictions generated successfully: {predictions.count()} rows.")
    display(predictions)
    
    # Save predictions to a table
    predictions.write.mode("overwrite").saveAsTable(f"{DA.catalog_name}.{DA.schema_name}.prediction_table")
    print(f"Predictions saved to table: {DA.catalog_name}.{DA.schema_name}.prediction_table")
except Exception as e:
    raise ValueError(f"Failed to generate predictions: {e}")

# COMMAND ----------

# Evaluate the model
from pyspark.ml.evaluation import RegressionEvaluator

try:
    evaluator = RegressionEvaluator(
        labelCol="Diabetes_binary", 
        predictionCol="prediction", 
        metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE): {rmse}")
except Exception as e:
    raise ValueError(f"Failed to evaluate the model: {e}")

# Log evaluation metrics to MLflow
experiment_name = f"/Shared/{DA.username}_adv_mlops_demo_diabetes"
mlflow.set_experiment(experiment_name)

with mlflow.start_run():
    mlflow.log_param("environment", env)
    mlflow.log_metric("evaluation_rmse", rmse)
    print("Evaluation metrics logged to MLflow.")
