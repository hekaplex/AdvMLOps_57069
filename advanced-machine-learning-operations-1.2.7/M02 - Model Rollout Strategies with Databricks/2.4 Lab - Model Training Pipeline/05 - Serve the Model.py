# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')

# COMMAND ----------

from mlflow import MlflowClient

# Initialize the MLflow Client
client = <FILL_IN>

# Define the model name and alias
model_name = <FILL_IN>  # Replace with your actual model name
alias_a = "a" 
alias_b = "b"

# Get the model version by alias
model_a_version= <FILL_IN>
model_b_version = <FILL_IN>

# Print the model version
print(f"Version for model a: {model_a_version}")
print(f"Version for model b: {model_b_version}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient

try:
    # Initialize the workspace client
    workspace = WorkspaceClient()

    # Delete the serving endpoint
    workspace.serving_endpoints.delete(name=f"M02-endpoint_{schema}")
    print('Deleted Endpoint M02-endpoint')
except:
    print("Endpoint does not exist.")

# COMMAND ----------

# MAGIC %md
# MAGIC Setup the model serving to include both model versions with 40% of traffic going towards serving model A and 60% going towards model B.

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")
endpoint_name = f"M02-endpoint_{schema}"
spark.sql(f'use catalog {catalog}')
spark.sql(f'use schema {schema}')
# Check if the endpoint already exists
try:
    # Attempt to get the endpoint
    existing_endpoint = client.get_endpoint(endpoint_name)
    print(f"Endpoint '{endpoint_name}' already exists.")
except Exception as e:
    # If not found, create the endpoint
    if "RESOURCE_DOES_NOT_EXIST" in str(e):
        print(f"Creating a new endpoint: {endpoint_name}")
        endpoint = client.create_endpoint(
            name=endpoint_name,
            config={
                "served_entities": [
                    {
                        "name": "my-model-a",
                        "entity_name": <FILL_IN>,
                        "entity_version": <FILL_IN>,
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True
                    },
                    {
                        "name": "my-model-b",
                        "entity_name": <FILL_IN>,
                        "entity_version": <FILL_IN>,
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True
                    }
                ],
                "traffic_config": {
                    "routes": [
                        {
                            "served_model_name": <FILL_IN>,
                            "traffic_percentage": 30
                        },
                        {
                            "served_model_name": <FILL_IN>,
                            "traffic_percentage": 60
                        }
                    ]
                }
            }
        )
    else:
        print(f"An error occurred: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
