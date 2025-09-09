# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demonstration: Model Rollout Strategies with Mosaic AI Model Serving
# MAGIC
# MAGIC In this demonstration, we will explore how to perform the rollout strategy known as **A/B testing**. We will also give a brief discussion on how to implement the **Canary Model Rollout Strategy**, a method for releasing an application or service incrementally to a subset of users, as well as Blue Green Rollout. Using Python and Spark, all within the Databricks platform, we will showcase the ease with which we can use Mosaic AI Model Serving and other various tools and features for rollout strategies.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC - Understand the fundamentals of A/B testing.
# MAGIC - Learn to implement A/B testing frameworks using Spark for scalable data processing.
# MAGIC - Explore how to use Mosaic AI model serving to limit traffic and perform controlled experiments.
# MAGIC - Gain practical experience with Python and Databricks for real-world testing and deployment workflows.
# MAGIC
# MAGIC Through this session, you will see how these testing strategies can enhance the reliability and performance of machine learning models and applications in production environments.

# COMMAND ----------

# MAGIC %md
# MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE
# MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
# MAGIC Follow these steps to select the classic compute cluster:
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
# MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC    - In the drop-down, select **More**.
# MAGIC    - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC   
# MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
# MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
# MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
# MAGIC 1. Wait a few minutes for the cluster to start.
# MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need to use one of the following Databricks runtime(s): **16.3.x-cpu-ml-scala2.12**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC To get into the lesson, we first need to build some data assets and define some configuration variables required for this demonstration. When running the following cell, the output is hidden so our space isn't cluttered. To view the details of the output, you can hover over the next cell and click the eye icon. 
# MAGIC
# MAGIC The cell after the setup, titled `View Setup Variables`, displays the various variables that were created. You can click the Catalog icon in the notebook space to the right to see that your catalog was created with no data.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-2.3

# COMMAND ----------

# MAGIC %md
# MAGIC **Other Conventions:**
# MAGIC
# MAGIC Throughout this demo, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Dataset Location:  {DA.paths.datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Implement A/B Testing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Two Models for Testing
# MAGIC
# MAGIC Here we will read in our dataset and create two initial models. We will utilize MLflow for model tracking and register them to Unity Catalog and provide aliases to separate the different models. We will imagine that we're asked to determine if one model performs better with including two features: `HvyAlcoholConsump` and `HighChol`. We will label model `A` (our control group) as including all features from our baseline `diabetes` dataset while model `B` will not include these two features. We will provide model `A` with alias `@a` and model `B` with alias `@b` in **schema** under **models**.

# COMMAND ----------

# DBTITLE 1,Configure MLflow
import mlflow

mlflow.set_registry_uri ("databricks-uc")
# set the path for mlflow experiment
mlflow.set_experiment(f"/Users/{DA.username}/{DA.schema_name}_model")

# COMMAND ----------

# DBTITLE 1,Setup Test Train Datasets
from mlflow.models.signature import infer_signature
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Load dataset
df = spark.read.format('delta').table('diabetes')
training_df = df.toPandas()

included_features_list = [c for c in df.columns if c not in ["Diabetes_binary",'id']]
smaller_included_features_list = [c for c in included_features_list if c not in ["HvyAlcoholConsump", "HighChol"]]

# Split the data into train and test sets
X_large = training_df[included_features_list]
X_small = training_df[smaller_included_features_list]
y = training_df["Diabetes_binary"]
X_large_train, X_large_test, y_large_train, y_large_test = train_test_split(X_large, y, test_size=0.2, random_state=42)
X_small_train = X_large_train.drop(columns=["HighChol", "HvyAlcoholConsump"])
X_small_test = X_large_test.drop(columns=["HighChol", "HvyAlcoholConsump"])
y_small_train = y_large_train
y_small_test = y_large_test

# COMMAND ----------

# DBTITLE 1,Train the ML Model using MLflow and Register to UC
from mlflow import MlflowClient
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
        registered_model_name = f"{DA.catalog_name}.{DA.schema_name}.my_model_{DA.unique_name('-')}"

        mlflow.sklearn.log_model(
        rf_classifier,
        artifact_path = "model-artifacts", 
        input_example=X_train[:3],
        signature=infer_signature(X_train, y_large_train)
        )

        model_uri = f"runs:/{run.info.run_id}/model-artifacts"

    mlflow.set_registry_uri("databricks-uc")

    # Define the model name 
    model_name = f"{DA.catalog_name}.{DA.schema_name}.my_model_{DA.unique_name('-')}"

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
# MAGIC ## Mosaic AI Model Serving
# MAGIC
# MAGIC Next, we need to determine how to deploy these models and how to direct traffic. Databricks Mosaic AI Model Serving makes this really simple to do. Let's setup a model serving endpoint and direct traffic to point 50% to one model and 50% to the other.

# COMMAND ----------

# DBTITLE 1,Get the Model Version
from mlflow import MlflowClient

# Initialize the MLflow Client
client = MlflowClient()

# Define the model name and alias
model_name = f"{DA.catalog_name}.{DA.schema_name}.my_model_{DA.unique_name('-')}" # Replace with your actual model name
alias_a = "a" 
alias_b = "b"

# Get the model version by alias
model_a_version= client.get_model_version_by_alias(model_name, alias_a).version
model_b_version = client.get_model_version_by_alias(model_name, alias_b).version

# Print the model version
print(f"Version for model a: {model_a_version}")
print(f"Version for model b: {model_b_version}")

# COMMAND ----------

# DBTITLE 1,Clear out the model Serving Endpoint if it exists
from databricks.sdk import WorkspaceClient

try:
    # Initialize the workspace client
    workspace = WorkspaceClient()

    # Delete the serving endpoint
    workspace.serving_endpoints.delete(name=f"M02-endpoint_{DA.schema_name}")
except:
    print("Endpoint does not exist.")

# COMMAND ----------

from mlflow.deployments import get_deploy_client
import time

client = get_deploy_client("databricks")
endpoint_name = f"M02-endpoint_{DA.schema_name}"
spark.sql(f'use catalog {DA.catalog_name}')
spark.sql(f'use schema {DA.schema_name}')

def wait_for_endpoint(endpoint_name, timeout=1200, interval=200):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            endpoint_status = client.get_endpoint(endpoint_name)
            if endpoint_status["state"]["ready"]:
                print(f"Endpoint '{endpoint_name}' is ready.")
                return
        except Exception as e:
            if "RESOURCE_DOES_NOT_EXIST" in str(e):
                print(f"Endpoint '{endpoint_name}' does not exist yet.")
            else:
                print(f"An error occurred: {e}")
        time.sleep(interval)
    print(f"Timeout: Endpoint '{endpoint_name}' is not ready after {timeout} seconds.")
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
                        "entity_name": model_name,
                        "entity_version": model_a_version,
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True
                    },
                    {
                        "name": "my-model-b",
                        "entity_name": model_name,
                        "entity_version": model_b_version,
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True
                    }
                ],
                "traffic_config": {
                    "routes": [
                        {
                            "served_model_name": "my-model-a",
                            "traffic_percentage": 50
                        },
                        {
                            "served_model_name": "my-model-b",
                            "traffic_percentage": 50
                        }
                    ]
                }
            }
        )
        wait_for_endpoint(endpoint_name)
    else:
        print(f"An error occurred: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Querying the endpoint
# MAGIC
# MAGIC We now simulate how you perform A/B testing with Mosaic AI Model Serving. Recall that model B has two fewer features than model A. So, when we send in the query, we want to make sure that we're looking at the larger test set instead of the smaller one. The endpoint will ignore features that don't apply to model B and will use all of the features that apply to model A. 
# MAGIC
# MAGIC In this step we will inference on batches of the test dataset while monitoring which model is being served so we can see the split. 
# MAGIC
# MAGIC **Warning: It may take a few moments for your endpoint to deploy**

# COMMAND ----------

# DBTITLE 1,Run Batch Inference and Monitor Model Served Frequency
import pandas as pd
import time
from mlflow.deployments import get_deploy_client

# Initialize Databricks client
client = get_deploy_client("databricks")
w = WorkspaceClient()

# Function to wait for model serving endpoint to be fully READY
def wait_for_fully_ready_endpoint(endpoint_name, timeout=1800, interval=200):
    """
    Waits until the endpoint is fully READY, checking its status every `interval` seconds.
    
    Args:
        endpoint_name (str): The name of the endpoint.
        timeout (int): Maximum time (seconds) to wait.
        interval (int): Time (seconds) between checks.

    Returns:
        bool: True if the endpoint is READY, False if timeout occurs.
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            endpoint_status = client.get_endpoint(endpoint_name)
            state = endpoint_status.get("state", {})

            ready_state = state.get("ready", "NOT_READY")
            config_update_state = state.get("config_update", "IN_PROGRESS")

            print(f"Waiting for endpoint '{endpoint_name}' to be READY... [Status: {ready_state}, Config: {config_update_state}]")

            if ready_state == "READY" and config_update_state == "NOT_UPDATING":
                print(f"Endpoint '{endpoint_name}' is now fully READY.")
                return True

        except Exception as e:
            print(f"Error checking endpoint status: {e}")

        time.sleep(interval)

    print(f"Timeout: Endpoint '{endpoint_name}' was not fully ready after {timeout} seconds.")
    return False

# Ensure the endpoint is fully ready before querying
if wait_for_fully_ready_endpoint(endpoint_name):
    print(f"Endpoint '{endpoint_name}' is fully ready. Proceeding with inference.")
else:
    raise RuntimeError(f" Endpoint '{endpoint_name}' is not fully available. Please check deployment.")

# Initialize an empty DataFrame to store results
results_df = pd.DataFrame(columns=["prediction", "model_served"])
requests = X_large_test.reset_index()

batch_size = 10
number_of_batches = 5

# Perform inference on batches of the test dataset
for batch_num in range(number_of_batches):
    try:
        # Fetch the batch of requests
        batch = payload(requests[requests.index // batch_size == batch_num].to_dict(orient='split'))
        
        # Query the serving endpoint
        query_response = w.serving_endpoints.query(name=endpoint_name, dataframe_split=batch)

        # Print batch details
        print(f"Batch {batch_num + 1} response:")
        print(batch)
        print(query_response)

        # Extract predictions and model served information
        batch_results = pd.DataFrame({
            "prediction": query_response.predictions,
            "model_served": query_response.served_model_name
        })

        # Append batch results to the main DataFrame
        results_df = pd.concat([results_df, batch_results], ignore_index=True)

    except Exception as e:
        print(f"Error processing batch {batch_num + 1}: {e}")
        continue  # Skip to the next batch

# Display the final results DataFrame
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### View Data about the Served Model
# MAGIC In addition to the frequency of the model served, we can view latency using the UI. Navigate to the model serving endpoint in **Serving** and scroll down to view Metrics. Here you will find Latency, Request Rate, Request Error, CPU Usage, Memory Usage, and Provisioned Concurrency. All of these should be taken into account when making a determination of rolling out a new model. 
# MAGIC
# MAGIC In the **Events** tab you will see the **Timestamp** along with **Event type**, **Served entity name**, and **message**. This can be useful for transparency in rolling out the model to determine how long it takes for any particular event. 
# MAGIC
# MAGIC Additionally, you can view the **Logs** of the serving model.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric Tracking
# MAGIC
# MAGIC Now that we have established our different models as well as setup our hypothesis, let's take a moment to establish which metrics we will be tracking. Since we are using a random forest classifier, some important metrics include F1-score, precision, recall, accuracy, and AUC-ROC. The table below summarizes the metric, its use case and focus area. We will not go into analysis here, as that is covered in a separate demonstration. 
# MAGIC
# MAGIC ### Suggested Tracking Plan
# MAGIC
# MAGIC | **Metric**              | **Use Case**                                  | **Focus Area**                  |
# MAGIC |--------------------------|-----------------------------------------------|----------------------------------|
# MAGIC | **F1-Score**            | Overall balance between precision and recall | Primary metric for A/B testing  |
# MAGIC | **Precision and Recall** | Insights into specific model behaviors       | For deeper performance insights |
# MAGIC | **AUC-ROC or PR AUC**   | Discrimination ability and imbalance focus   | Secondary evaluation            |
# MAGIC | **Log Loss**            | Prediction confidence                        | Confidence assessment           |
# MAGIC | **Feature Importance**  | New feature contribution                     | Validate feature utility        |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Rollout Strategies
# MAGIC
# MAGIC We close this demo with describing two additional common rollout strategies that we will go into due to time constraints. However, it is worth describing how Mosaic AI can still solve these problems.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adaptation to Blue-Green Rollout
# MAGIC
# MAGIC Using Mosaic AI Model Serving makes it effortless to switch between different served models. Recall that **Blue-green testing** is a deployment strategy that minimizes the downtime to reduce the risk during the release of new features or updates to applications. 
# MAGIC
# MAGIC **Case 1: Zero Downtime**
# MAGIC
# MAGIC If we need 0 downtime, we can simply have two endpoints deployed and expose the application to the correct API and immediately delete the old model serving endpoint. We would maintain both versions of the code but only delete the older one once the new is in use. 
# MAGIC
# MAGIC **Case 2: A Few Minutes Downtime**
# MAGIC
# MAGIC If we are allowed to have a few minutes of downtime, we can use the same model serving endpoint and update the version being served by using the following code. Note that this may take a few minutes to update, but we only have a single model serving endpoint deployed in this case. 
# MAGIC
# MAGIC To summarize, with zero downtime, we must maintain two separate model serving endpoints, which can be costly. If we allow for a few minutes of downtime, then we only have to maintain a single endpoint and simply perform the switch when we are ready. In the latter case, we don't need to maintain two endpoints or worry about cleaning up resources. With Mosaic AI Model Serving, the previous state of the endpoint is always running until the update to the new state is complete. This means the only element you have to adjust your rollout for is the time it takes to update to the new endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Adaptation to Canary Rollout
# MAGIC
# MAGIC Using Mosaic AI Model Serving, you can perform a gradual rollout by controlling the traffic directed to each model version. Unlike in A/B testing, where we might use a fixed 50/50 split to compare model versions, a canary rollout involves progressively increasing the traffic to the new model. For example, you could start with a 10/90 split (10% to the new model and 90% to the current model), then adjust to a 30/70 split, and so on, until the new model handles 100% of the traffic.
# MAGIC
# MAGIC This gradual approach ensures the new model can be validated at scale without sacrificing uptime. Additionally, unlike the blue-green rollout strategy, a canary rollout does not require maintaining separate endpoints. Instead, traffic routing is dynamically adjusted, making it a cost-effective and low-risk strategy for rolling out updates.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this demonstration you learned about how to utilize MLflow along with Unity Catalog and Mosaic AI to enforce various rollout strategies with an emphasis on A/B testing. In addition, you learned how providing an alias can help keep track of which model you wish to rollout or test against. Finally, you learned how Mosaic AI allows for split traffic to make rollout strategies like blue-green and canary painless by simply updating the model serving endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
