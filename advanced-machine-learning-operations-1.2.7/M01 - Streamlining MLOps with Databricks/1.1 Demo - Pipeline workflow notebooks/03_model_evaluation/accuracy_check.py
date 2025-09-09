# Databricks notebook source
# MAGIC %md
# MAGIC # Model Evaluation with Accuracy Check
# MAGIC This notebook evaluates the trained model's accuracy and determines if the pipeline should proceed or stop based on a threshold.
# MAGIC

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
dbutils.widgets.text("accuracy_threshold", "0.6", "Accuracy Threshold")
dbutils.widgets.text("model_name", "diabetes_model_dev", "Model Name")

# Get values from widgets
accuracy_threshold = float(dbutils.widgets.get("accuracy_threshold"))
model_name = dbutils.widgets.get("model_name")

# COMMAND ----------

# Evaluate model predictions
from pyspark.ml.evaluation import BinaryClassificationEvaluator

evaluator = BinaryClassificationEvaluator(
    labelCol="Diabetes_binary", 
    rawPredictionCol="prediction", 
    metricName="areaUnderROC"
)

# COMMAND ----------

try:
    # Load the predictions
    predictions = spark.sql(f"SELECT * FROM {DA.catalog_name}.{DA.schema_name}.prediction_table")
    
    # Evaluate the accuracy
    accuracy = evaluator.evaluate(predictions)
    display(f"Model accuracy: {accuracy}")
    
    # Check accuracy threshold
    if accuracy >= accuracy_threshold:
        display(f"Model accuracy {accuracy} meets the threshold {accuracy_threshold}. Proceeding with the pipeline.")
        dbutils.notebook.exit("SUCCESS")
    else:
        display(f"Model accuracy {accuracy} is below the threshold {accuracy_threshold}. Stopping the pipeline.")
        dbutils.notebook.exit("FAILURE")
except Exception as e:
    raise ValueError(f"Failed to evaluate the model accuracy: {e}")
