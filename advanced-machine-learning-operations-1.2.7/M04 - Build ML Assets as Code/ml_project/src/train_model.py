# Databricks notebook source
# MAGIC %md
# MAGIC # Model Training
# MAGIC This notebook trains a machine learning model and registers it in MLflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script.

# COMMAND ----------

# MAGIC %run ./Classroom-Setup

# COMMAND ----------

# Import necessary libraries
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from mlflow.models.signature import infer_signature
from mlflow.tracking import MlflowClient
import mlflow

# Initialize success flag
success_flag = True

try:
    # Step 1: Widgets for environment-specific configurations
    dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment Name")
    env = dbutils.widgets.get("env")
    
    # Step 2: Read the feature-engineered data
    try:
        feature_data_path = f"{DA.catalog_name}.{DA.schema_name}.feature_engineered_data"
        feature_data = spark.read.format("delta").table(feature_data_path)
        print("Feature-engineered data loaded successfully.")
    except Exception as e:
        print(f"Error reading feature-engineered data: {e}")
        success_flag = False
        raise
    
    # Step 3: Assemble features for ML model
    try:
        assembler = VectorAssembler(
            inputCols=[
                "HighBP", "BMI", "Smoker", "PhysActivity", "Fruits",
                "Veggies", "MentHlth_squared", "BMI_squared",
                "BMI_MentHlth_interaction", "Age"
            ],
            outputCol="features"
        )
        feature_data = assembler.transform(feature_data)
        print("Feature vector assembled successfully.")
    except Exception as e:
        print(f"Error during feature assembly: {e}")
        success_flag = False
        raise
    
    # Step 4: Split the dataset into training and test sets
    try:
        train_data, test_data = feature_data.randomSplit([0.8, 0.2], seed=42)
        print("Data split into training and test sets successfully.")
    except Exception as e:
        print(f"Error during data splitting: {e}")
        success_flag = False
        raise
    
    # Step 5: Train a Random Forest model
    try:
        rf = RandomForestRegressor(featuresCol="features", labelCol="Diabetes_binary")
        model = rf.fit(train_data)
        print("Model trained successfully.")
    except Exception as e:
        print(f"Error during model training: {e}")
        success_flag = False
        raise
    
    # Step 6: Evaluate the model
    try:
        evaluator = RegressionEvaluator(labelCol="Diabetes_binary", predictionCol="prediction", metricName="rmse")
        predictions = model.transform(test_data)
        rmse = evaluator.evaluate(predictions)
        print(f"Model evaluation completed. RMSE: {rmse}")
    except Exception as e:
        print(f"Error during model evaluation: {e}")
        success_flag = False
        raise
    
    # Step 7: Register the model in MLflow
    try:
        experiment_name = f"/Shared/{DA.username}_adv_mlops_demo_diabetes"
        mlflow.set_experiment(experiment_name)
        
        signature = infer_signature(
            train_data.select("features").toPandas(),
            model.transform(train_data).select("prediction").toPandas()
        )
        
        model_base_name = f"diabetes_model_{env}"
        full_model_name = f"{DA.catalog_name}.{DA.schema_name}.{model_base_name}"
        
        with mlflow.start_run():
            mlflow.log_param("environment", env)
            mlflow.log_metric("rmse", rmse)
            mlflow.spark.log_model(
                model,
                artifact_path="model",
                registered_model_name=full_model_name,
                signature=signature
            )
        print(f"Model registered in MLflow: {full_model_name}")
    except Exception as e:
        print(f"Error during model registration: {e}")
        success_flag = False
        raise
    
    # Step 8: Ensure the model is registered and available in MLflow
    try:
        client = MlflowClient()
        model_version_infos = client.search_model_versions(f"name = '{full_model_name}'")
        if model_version_infos:
            latest_version = max([int(info.version) for info in model_version_infos])
            model_uri = f"models:/{full_model_name}/{latest_version}"
            loaded_model = mlflow.spark.load_model(model_uri)
            print(f"Model registered and loaded for environment: {env}")
        else:
            print(f"Model {full_model_name} is not registered.")
            success_flag = False
    except Exception as e:
        print(f"Error verifying model registration: {e}")
        success_flag = False
        raise

except Exception:
    success_flag = False
    print("Notebook exited: FAILURE")
    dbutils.notebook.exit("FAILED")

# Final output
if success_flag:
    print("Notebook exited: SUCCESS")
    dbutils.notebook.exit("SUCCESS")
else:
    dbutils.notebook.exit("FAILED")
