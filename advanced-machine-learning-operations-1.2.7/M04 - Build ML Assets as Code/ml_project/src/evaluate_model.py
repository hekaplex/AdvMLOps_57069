# Databricks notebook source
# MAGIC %md
# MAGIC # Model Evaluation and Testing
# MAGIC This notebook evaluates the performance of the trained model, validates predictions, and prints the accuracy score.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run ./Classroom-Setup

# COMMAND ----------

import json

# Widgets for environment-specific configurations
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment Name")
dbutils.widgets.text("model_name", "diabetes_model_dev", "Model Name")
dbutils.widgets.text("accuracy_threshold", "0.6", "Accuracy Threshold")

# Initialize output dictionary
output_results = {}

# Get environment and model name from widgets
env = dbutils.widgets.get("env")
model_name = dbutils.widgets.get("model_name")
accuracy_threshold = float(dbutils.widgets.get("accuracy_threshold"))

# Define the catalog, schema, and table paths
feature_data_path = f"{DA.catalog_name}.{DA.schema_name}.feature_engineered_data"
full_model_name = f"{DA.catalog_name}.{DA.schema_name}.{model_name}"

# Load feature-engineered data
try:
    feature_data = spark.table(feature_data_path)
    feature_count = feature_data.count()
    if feature_count == 0:
        raise ValueError("The feature-engineered dataset is empty. Ensure the data transformation step completed successfully.")
    else:
        print(f"Feature data contains {feature_count} rows.")
        output_results["feature_data_count"] = feature_count
        feature_data.printSchema()
except Exception as e:
    raise ValueError(f"Failed to load feature-engineered data: {e}")

# Ensure required columns are present
required_columns = [
    "HighBP", "BMI", "Smoker", "PhysActivity", "Fruits", 
    "Veggies", "MentHlth_squared", "BMI_squared", 
    "BMI_MentHlth_interaction", "Age"
]
missing_columns = [col for col in required_columns if col not in feature_data.columns]
if missing_columns:
    raise ValueError(f"Missing columns in feature_data: {missing_columns}")

# Create feature vector
from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
    inputCols=required_columns,
    outputCol="features",
    handleInvalid="skip"
)

try:
    feature_data = assembler.transform(feature_data)
    vector_count = feature_data.count()
    if vector_count == 0:
        raise ValueError("The feature_data DataFrame is empty after vector assembly. Check the input data and transformation logic.")
    print(f"Feature vector successfully created with {vector_count} rows.")
    output_results["feature_vector_count"] = vector_count
except Exception as e:
    raise ValueError(f"Vector assembly failed: {e}")

# Verify model compatibility
from mlflow.tracking import MlflowClient
import mlflow

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

# Generate predictions
try:
    predictions = model.transform(feature_data)
    prediction_count = predictions.count()
    if prediction_count == 0:
        raise ValueError("The model failed to generate any predictions. Check the input features and model compatibility.")
    print(f"Predictions generated successfully: {prediction_count} rows.")
    output_results["predictions_count"] = prediction_count
    
    predictions.write.mode("overwrite").saveAsTable(f"{DA.catalog_name}.{DA.schema_name}.prediction_table")
    print(f"Predictions saved to table: {DA.catalog_name}.{DA.schema_name}.prediction_table")
except Exception as e:
    raise ValueError(f"Failed to generate predictions: {e}")

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
    output_results["rmse"] = rmse
except Exception as e:
    raise ValueError(f"Failed to evaluate the model: {e}")

# Evaluate model predictions
from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(
    labelCol="Diabetes_binary", 
    rawPredictionCol="prediction", 
    metricName="areaUnderROC"
)

try:
    accuracy = evaluator.evaluate(predictions)
    print(f"Model accuracy: {accuracy}")
    output_results["model_accuracy"] = accuracy
except Exception as e:
    raise ValueError(f"Failed to evaluate the model accuracy: {e}")

# Save output results to a JSON file
output_file_path = "./model_evaluation_output.json"
with open(output_file_path, "w") as f:
    json.dump(output_results, f, indent=4)

print(f"Model Evaluation Output Results:\n{json.dumps(output_results, indent=4)}")
print(f"Output results saved to {output_file_path}")

# Final decision
if accuracy >= accuracy_threshold:
    print("Notebook exited: SUCCESS")
    dbutils.notebook.exit("SUCCESS")
else:
    print("Notebook exited: FAILURE")
    dbutils.notebook.exit("FAILURE")
