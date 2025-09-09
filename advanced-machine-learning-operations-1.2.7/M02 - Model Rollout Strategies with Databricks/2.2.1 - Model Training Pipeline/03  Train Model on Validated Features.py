# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Notebook - Train Model on Validated Features
# MAGIC
# MAGIC 1. **Purpose of the Notebook**:
# MAGIC    - In this third notebook, called **Train Model on Features**, we will train a machine learning model using the **validated features** from the previous notebook.
# MAGIC
# MAGIC 2. **Process**:
# MAGIC    - The validated feature table is read and used as input for model training.
# MAGIC    - The resulting model is stored in **Unity Catalog** for centralized management and accessibility.
# MAGIC
# MAGIC 3. **Next Steps**:
# MAGIC    - After training, the next logical step is to **validate the model** by evaluating its performance using metrics such as accuracy, precision, recall, or F1-score, ensuring its readiness for deployment.

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
primary_key = dbutils.widgets.get('primary_key')
target_column = dbutils.widgets.get('target_column')
username = dbutils.widgets.get('username')
silver_table_name = dbutils.widgets.get('silver_table_name')

# COMMAND ----------

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

import mlflow
from mlflow.models.signature import infer_signature

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Load dataset
df = spark.read.format('delta').table(f'{silver_table_name}_features')

# Read in the original dataset as well and join with df
original_df = spark.read.format('delta').table(f'{silver_table_name}').select(primary_key, target_column)
df = df.join(original_df, on=primary_key)

training_df = df.toPandas()

X = training_df.drop([primary_key, target_column], axis=1)
y = training_df[target_column]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)



# set the path for mlflow experiment
mlflow.set_experiment(f"/Users/{username}/{schema}_model")

with mlflow.start_run(run_name = 'mlflow-run') as run:  
    # Initialize the Random Forest classifier
    rf_classifier = RandomForestClassifier(random_state=42)

    # Fit the model on the training data
    rf_classifier.fit(X_train, y_train)

    # Make predictions on the test data
    y_pred = rf_classifier.predict(X_test)

    # Enable automatic logging of input samples, metrics, parameters, and models
    mlflow.sklearn.autolog(
        log_input_examples = True,
        silent = True
    )
        
    mlflow.sklearn.log_model(
        rf_classifier,
        artifact_path = "model-artifacts", 
        input_example=X_train[:3],
        signature=infer_signature(X_train, y_train)
    )

    model_uri = f"runs:/{run.info.run_id}/model-artifacts"

# COMMAND ----------

# Register the model in the model registry
registered_model = mlflow.register_model(model_uri=model_uri, name=f"{catalog}.{schema}.workflows_classifier_model")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
