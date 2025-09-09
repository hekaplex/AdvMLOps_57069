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

# COMMAND ----------


catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
primary_key = dbutils.widgets.get('primary_key')
target_column = dbutils.widgets.get('target_column')
silver_table_name = dbutils.widgets.get('silver_table_name')
delete_column = dbutils.widgets.get('delete_column')

# COMMAND ----------

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Setup Test Train Datasets
import mlflow
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Load dataset
df = spark.read.format('delta').table(silver_table_name)
training_df = df.toPandas()

included_features_list = [c for c in df.columns if c not in [target_column, primary_key]]
smaller_included_features_list = [c for c in included_features_list if c not in [delete_column]]

# Split the data into train and test sets
X_large = training_df[included_features_list]
X_small = training_df[smaller_included_features_list]
y = training_df[target_column]
X_large_train, X_large_test, y_large_train, y_large_test = train_test_split(X_large, y, test_size=0.2, random_state=42)
X_small_train = X_large_train.drop(columns=[delete_column])
X_small_test = X_large_test.drop(columns=[delete_column])
y_small_train = y_large_train
y_small_test = y_large_test

df = spark.createDataFrame(X_large_test)
df.write.format('delta').mode('overwrite').saveAsTable('X_large_test')

# COMMAND ----------

# DBTITLE 1,Train the ML Model using MLflow and Register to UC
import mlflow
from mlflow.tracking import MlflowClient
def train_model(X_train,y, alias):
    # Start MLflow run
    with mlflow.start_run(run_name='mlflow-run') as run:
        # Initialize the Random Forest classifier
        rf_classifier = RandomForestClassifier(random_state=42)

        # Fit the model on the training data
        rf_classifier.fit(X_train, y_large_train)

        # Enable autologging
        mlflow.sklearn.autolog(log_input_examples=True, silent=True)

        # Define the registered model name
        registered_model_name = f"{catalog}.{schema}.my_model_{schema}"

        mlflow.sklearn.log_model(
        rf_classifier,
        artifact_path = "model-artifacts", 
        input_example=X_train[:3],
        signature=infer_signature(X_train, y_large_train)
        )

        model_uri = f"runs:/{run.info.run_id}/model-artifacts"

    mlflow.set_registry_uri("databricks-uc")

    # Define the model name 
    model_name = f"{catalog}.{schema}.my_model_{schema}"

    # Register the model in the model registry
    registered_model = mlflow.register_model(model_uri=model_uri, name=model_name)

    # Initialize an MLflow Client
    client = MlflowClient()

    # Assign an alias
    client.set_registered_model_alias(
        name= registered_model.name,  # The registered model name
        alias=alias,  # The alias representing the dev environment
        version=registered_model.version  # The version of the model you want to move to "dev"
    )

train_model(X_large_train,y_large_train, 'a')
train_model(X_small_train,y_small_train, 'b')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
