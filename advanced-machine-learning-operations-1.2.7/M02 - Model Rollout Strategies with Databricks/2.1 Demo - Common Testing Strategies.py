# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demonstration - Common Testing Strategies
# MAGIC
# MAGIC In this demonstration, you will investigate common pipeline testing strategies for data science and machine learning. In addition, you will be given an introduction to **unittest**, a common testing framework. You will gain a new perspective of MLflow as an integral tool for proper model testing and important to modern MLOps pipelines. 
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC
# MAGIC By the end of this demonstration, you will be able to do the following:
# MAGIC
# MAGIC - Understand the differences between different pipeline tests and how to build helper functions to validate the following types of tests:
# MAGIC     - Data Validation
# MAGIC     - Data Transformation
# MAGIC     - Model Integration
# MAGIC     - Modeling Functions
# MAGIC         - Testing model functions will utilize MLflow and Unity Catalog for comprehensive versioning and lineage. 
# MAGIC - Understand **unittest** as a comprehensive testing framework 
# MAGIC
# MAGIC **🚨Warning: Some of the cells are meant to fail for demonstration purposes.**
# MAGIC

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

# MAGIC %run ../Includes/Classroom-Setup-2.1

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
# MAGIC ## Further Preparation - Train, Register, and Serve ML model
# MAGIC
# MAGIC For testing that our model is behaving as intended, we will train and package our model with MLflow, register it to Unity Catalog, serve the model using Mosaic AI Model Serving. 
# MAGIC
# MAGIC **Warning: It will take a few minutes to setup the Model Serving Endpoint.**

# COMMAND ----------

# DBTITLE 1,Set MLflow Configuration
import mlflow
# Modify the registry uri to point to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Define the model name 
model_name = f"{DA.catalog_name}.{DA.schema_name}.{DA.username}"

# COMMAND ----------

model_name

# COMMAND ----------

# DBTITLE 1,Train an ML Model
from mlflow.models.signature import infer_signature

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split

# Load dataset
df = spark.read.format('delta').table('diabetes')
training_df = df.toPandas()

X = training_df.drop(["id", "Diabetes_binary"], axis=1)
y = training_df["Diabetes_binary"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)



# set the path for mlflow experiment
mlflow.set_experiment(f"/Users/{DA.username}/{DA.username}_model")

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

# DBTITLE 1,Register the Model with UC
# Register the model in the model registry
registered_model = mlflow.register_model(model_uri=model_uri, name=f"{DA.catalog_name}.{DA.schema_name}.testing_strats_model")

# COMMAND ----------

# DBTITLE 1,Delete old endpoint
from databricks.sdk import WorkspaceClient

try:
    # Initialize the workspace client
    workspace = WorkspaceClient()

    # Delete the serving endpoint
    workspace.serving_endpoints.delete(name=f"M02-endpoint_strats_{DA.schema_name}")
    print('Deleted Endpoint M02-endpoint')
except:
    print(f"Endpoint M02-endpoint_{DA.schema_name} does not exist.")

# COMMAND ----------

# DBTITLE 1,Serve the model
from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")
endpoint_name = f"M02-endpoint_strats_{DA.schema_name}"
endpoint_name = endpoint_name.replace("@databricks.com", "").replace('.', '-')
spark.sql(f'use catalog {DA.catalog_name}')
spark.sql(f'use schema {DA.schema_name}')
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
                        "name": "strats-model",
                        "entity_name": f"{DA.catalog_name}.{DA.schema_name}.testing_strats_model",
                        "entity_version": 1,
                        "workload_size": "Small",
                        "scale_to_zero_enabled": True
                    },
                ],
                "traffic_config": {
                    "routes": [
                        {
                            "served_model_name": "strats-model",
                            "traffic_percentage": 100
                        }
                    ]
                }
            }
        )
    else:
        print(f"An error occurred: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Common ML Pipeline Testing
# MAGIC
# MAGIC Here we will go over common ML pipeline testing paradigms that can be used to ensure robust and reliable ML models by testing various aspects of the ML pipeline. We will focus on the following:
# MAGIC 1. Data Validation - data quality checks, expected pattern checks, and custom business logic are enforced.
# MAGIC 1. Data Transformations - apply data transformations like normalization and encoding for feature engineering tasks.
# MAGIC 1. Modeling functions - unit and integration tests are used to test individual component interactions as well as overall end-to-end testing.
# MAGIC 1. Model Integration - CI/CD integration along with MLOps workflows to enable long-term efficiency of ML systems
# MAGIC
# MAGIC When exploring each of these components of testing, we will provide helper functions. It should be noted that this is not a comprehensive dive into each of these topics.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Validation
# MAGIC Data Validation for ML Pipeline Testing involves ensuring that the input, intermediate, and output data in a machine learning pipeline meet expected quality, structure, and distribution standards. It is a critical step to verify that the data being processed in an ML pipeline aligns with the assumptions made during model development, ensuring the reliability and correctness of the pipeline. Here we will verify our dataframe schema, that no values are missing, and that non-negative values do not exist.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Helper Functions
# MAGIC
# MAGIC These functions provide validation checks for (PySpark) DataFrames to ensure data quality and consistency. 
# MAGIC
# MAGIC - `validate_schema` ensures the schema matches an expected definition.
# MAGIC - `validate_no_missing_values` checks for and reports any missing (null) values in the DataFrame.
# MAGIC - `validate_binary_column` confirms that a specified column contains only binary values (0 or 1).
# MAGIC - `validate_double_column` verifies that a specific column’s data type is double.
# MAGIC - `validate_non_negative_values` ensures that all columns in the DataFrame contain only non-negative values. 
# MAGIC
# MAGIC Overall, these function help to enforce data integrity for downstream machine learning or analytical tasks.

# COMMAND ----------

# DBTITLE 1,Helper functions
def validate_schema(df, expected_schema):
    """
    Validates whether the schema of the given DataFrame matches the expected schema.

    Parameters:
    - df: The PySpark DataFrame to check.
    - expected_schema: The expected schema (StructType).

    Uses:
    - AssertionError if the schema does not match.
    """

    actual_schema = df.schema
    assert actual_schema == expected_schema, (
        f"Schema validation failed.\n"
        f"Expected schema: {expected_schema}\n"
        f"Actual schema: {actual_schema}"
    )
    print("Schema validation passed!")

def validate_no_missing_values(df):
    """
    Validates that the given PySpark DataFrame contains no missing (null) values.

    Parameters:
    - df: The PySpark DataFrame to check.

    Uses:
    - AssertionError if missing values are found.
    """
    from pyspark.sql.functions import col, sum

    missing_values = df.agg(*[
        sum(col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).collect()[0].asDict()

    missing_columns = {col: missing_values[col] for col in df.columns if missing_values[col] > 0}

    assert not missing_columns, (
        f"Missing values found in the following columns: {missing_columns}"
    )
    print("No missing values detected!")

def validate_binary_column(df, column_name):
    """
    Validates that the specified column in the DataFrame contains only binary values (0 or 1).

    Parameters:
    - df: The PySpark DataFrame to check.
    - column_name: The name of the column to validate.

    Uses:
    - AssertionError if the column contains non-binary values.
    """
    # Find distinct values in the column
    distinct_values = df.select(column_name).distinct().rdd.flatMap(lambda x: x).collect()

    # Check if all distinct values are in the set {0, 1}
    is_binary = set(distinct_values).issubset({0, 1})

    assert is_binary, (
        f"Column '{column_name}' contains non-binary values: {set(distinct_values)}"
    )
    print(f"Column '{column_name}' is binary (0 or 1).")


def validate_double_column(df, column_name):
    """
    Validates that the specified column in the DataFrame is of type double.

    Parameters:
    - df: The PySpark DataFrame to check.
    - column_name: The name of the column to validate.

    Uses:
    - AssertionError if the column is not of type double.
    """
    # Get the data type of the column
    column_data_type = df.schema[column_name].dataType

    # Check if the data type is DoubleType
    is_double = isinstance(column_data_type, DoubleType)

    assert is_double, (
        f"Column '{column_name}' is not of type double. Found type: {column_data_type}"
    )
    print(f"Column '{column_name}' is of type double.")

def validate_non_negative_values(df):
    """
    Validates that all columns in the given PySpark DataFrame contain non-negative values (>= 0).

    Parameters:
    - df: The PySpark DataFrame to check.

    Uses:
    - AssertionError if any column contains negative values.
    """
    from pyspark.sql.functions import col, min

    negative_values = df.agg(*[
        min(col(c)).alias(c) for c in df.columns
    ]).collect()[0].asDict()

    negative_columns = {col: negative_values[col] for col in df.columns if negative_values[col] < 0}

    assert not negative_columns, (
        f"Negative values found in the following columns: {negative_columns}"
    )
    print("All columns contain non-negative values!")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define expected schema for our DataFrame

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, LongType, IntegerType

# Define the complete expected schema
expected_schema = StructType([
    StructField("Diabetes_binary", IntegerType(), True),
    StructField("HighBP", IntegerType(), True),
    StructField("HighChol", DoubleType(), True),
    StructField("CholCheck", DoubleType(), True),
    StructField("BMI", IntegerType(), True),
    StructField("Smoker", IntegerType(), True),
    StructField("Stroke", IntegerType(), True),
    StructField("HeartDiseaseorAttack", IntegerType(), True),
    StructField("PhysActivity", DoubleType(), True),
    StructField("Fruits", DoubleType(), True),
    StructField("Veggies", DoubleType(), True),
    StructField("HvyAlcoholConsump", DoubleType(), True),
    StructField("AnyHealthcare", DoubleType(), True),
    StructField("NoDocbcCost", DoubleType(), True),
    StructField("GenHlth", DoubleType(), True),
    StructField("MentHlth", DoubleType(), True),
    StructField("PhysHlth", DoubleType(), True),
    StructField("DiffWalk", DoubleType(), True),
    StructField("Sex", DoubleType(), True),
    StructField("Age", DoubleType(), True),
    StructField("Education", DoubleType(), True),
    StructField("Income", DoubleType(), True),
    StructField("id", LongType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC Let's now run our validation functions and debug any errors that arise.

# COMMAND ----------

# run validation tests
validate_schema(df, expected_schema)
validate_no_missing_values(df)
validate_binary_column(df, "HeartDiseaseorAttack")

# COMMAND ----------

validate_double_column(df, "Age")
validate_non_negative_values(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Oh no! We seem to have an AssertionError indicating there's been some faulty entry. It looks like there is a record within the HeartDiseaseorAttack feature that has a value of -1. After talking to the data team, it turns out there was some faulty logic (a typo if you can believe it) that converted this to -1 from 1. Let's clean this up and continue our validations.

# COMMAND ----------

display(df.agg({"HeartDiseaseorAttack": "max"}).collect()[0][0])

# COMMAND ----------

from pyspark.sql.functions import when
# convert values in HeartDiseaseorAttack that are -1 to 1
df = df.withColumn("HeartDiseaseorAttack", when(df.HeartDiseaseorAttack == -1, 1).otherwise(df.HeartDiseaseorAttack))

# COMMAND ----------

validate_binary_column(df, "HeartDiseaseorAttack")

# COMMAND ----------

validate_double_column(df, "Age")
validate_non_negative_values(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformations
# MAGIC
# MAGIC Transformation testing involved validating and verifying the operations applied to raw data to prepare it for use in a machine learning pipeline. These transformations include cleaning, scaling, encoding, and feature extraction, which are critical for ensuring that the data fed into the model aligns with the intended assumptions. Here we will consider performing a normalization test on the `Age` feature and validating that the transformation has been applied correctly.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Helper Functions
# MAGIC
# MAGIC These helper functions ensure proper normalization of a DataFrame column and validate the process. 
# MAGIC
# MAGIC - `normalize_column` creates a normalized version of a specified column by subtracting its mean and dividing by its standard deviation, appending the result as a new column in the DataFrame.
# MAGIC - `test_column_normalized` verifies the correctness of the normalization by checking if the resulting column’s mean is approximately 0 and its standard deviation is approximately 1, within a small tolerance, to confirm accurate scaling for downstream analysis or modeling.

# COMMAND ----------

# This function will normalize the dataframe's column
def normalize_column(df, column):
    if column not in df.columns:
        raise AssertionError(f"Column '{column}' does not exist in the DataFrame.")
    
    # Simulate a normalized column for demonstration
    df[f'{column}_normalized'] = (df[column] - df[column].mean()) / df[column].std()
    return df

# Test function to check normalization
def test_column_normalized(df, column):
    if column not in df.columns:
        raise AssertionError(f"Column '{column}' does not exist in the DataFrame.")
    
    mean = np.mean(df[column])
    std = np.std(df[column])
    
    # Allowing a small tolerance for floating-point arithmetic
    tolerance = 1e-4
    assert abs(mean) < tolerance, f"Mean of column '{column}' is not approximately 0. It is {mean}."
    assert abs(std - 1) < tolerance, f"Standard deviation of column '{column}' is not approximately 1. It is {std}."
    print(f"Column '{column}' is properly normalized.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Normalization Test

# COMMAND ----------

df = spark.read.format('delta').table('diabetes').toPandas()
df = normalize_column(df, 'Age')
test_column_normalized(df, 'Age_normalized')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Integration

# COMMAND ----------

# MAGIC %md
# MAGIC Copy and paste the url for the model serving endpoint. Locate your endpoint under **Serving** and click on it. Then copy the **URL**. Paste it in the following cell.

# COMMAND ----------

# DBTITLE 1,Grab the API URL and TOKEN
import os
# Retrieve the API URL and token using dbutils
API_URL = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}/serving-endpoints/M02-endpoint_strats_labuser10141814_1757436385/invocations"
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Helper Functions
# MAGIC
# MAGIC The function, `test_model_endpoint_status`, checks whether a model serving endpoint is operational by sending a test dataset as a request and verifying that it returns a 200 status code. It prepares the input dataset in the expected JSON format, sends it to the endpoint using a POST request with appropriate headers, and evaluates the response. If the status code is 200, it confirms the endpoint is functioning correctly; otherwise, it logs the error or response details. This function ensures the endpoint is ready to handle requests for predictions.

# COMMAND ----------

import requests
import json
def test_model_endpoint_status(dataset, url):
    """
    Test if the model serving endpoint returns a 200 status code.
    
    Args:
        dataset (pd.DataFrame): Input dataset to send to the model endpoint.
        url (string): The URL of the model serving endpoint.
    
    Returns:
        None. Prints the result of the test.
    """
    try:
        # Make a request to the model endpoint using the score_model function
        headers = {'Authorization': f'Bearer {TOKEN}', 'Content-Type': 'application/json'}
        
        # Prepare the data in the expected format
        ds_dict = {'dataframe_split': dataset.to_dict(orient='split')}
        data_json = json.dumps(ds_dict, allow_nan=True)
        
        # Send the request
        response = requests.request(method='POST', headers=headers, url=url, data=data_json)
        
        # Check the status code
        if response.status_code == 200:
            print("Test passed: Endpoint returned status code 200.")
        else:
            print(f"Test failed: Endpoint returned status code {response.status_code}. Response: {response.text}")
    except Exception as e:
        print(f"Test failed with error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test Endpoint Status

# COMMAND ----------

#grab the first row of the pandas dataframe X_test
X_test.iloc[[0]]

# COMMAND ----------

# MAGIC %md
# MAGIC Note that the following code might not run quickly since we set up our endpoint to scale to zero. If we run it again we will see much small latency.

# COMMAND ----------

# Call the test function in your Databricks notebook
test_model_endpoint_status(X_test.iloc[[0]], url = API_URL)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modeling Functions
# MAGIC
# MAGIC Model testing involves training, prediction, and evaluation of ML models as a part of the ML pipeline. Unit tests and integration tests are foundational components and should be considered separately. Here we will consider inference as an example to determine if the F1-score and accuracy are what we expect.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define Helper Functions
# MAGIC
# MAGIC The following function, `test_model_performance`, evaluates a logged MLflow model’s performance on a test dataset by calculating its F1-score and accuracy. It loads the model using its URI, generates predictions on the test features, and compares the calculated metrics against specified thresholds. If the F1-score or accuracy falls below the thresholds, the function raises an assertion error; otherwise, it confirms the model meets the performance criteria and prints the metrics. This ensures the model’s predictions align with expected performance levels before deployment or further use.

# COMMAND ----------

import mlflow.pyfunc
from sklearn.metrics import f1_score, accuracy_score

def test_model_performance(model_uri, X_test, y_test, f1_threshold=0.7, accuracy_threshold=0.8):
    """
    Test function to evaluate a logged MLflow model on a test dataset.
    
    Args:
        model_uri (str): URI of the logged MLflow model.
        X_test (pd.DataFrame): Test features.
        y_test (pd.Series): True labels for the test set.
        f1_threshold (float): Minimum acceptable F1-score.
        accuracy_threshold (float): Minimum acceptable accuracy score.
    
    Raises:
        AssertionError: If model performance metrics do not meet expected thresholds.
    """
    # Step 1: Load the model from MLflow
    model = mlflow.pyfunc.load_model(model_uri)
    
    # Step 2: Make predictions on the test set
    y_pred = model.predict(X_test)

    # Step 3: Calculate evaluation metrics
    f1 = f1_score(y_test, y_pred)
    accuracy = accuracy_score(y_test, y_pred)

    # Step 4: Assert performance thresholds
    assert f1 >= f1_threshold, f"F1-score {f1:.4f} is below the threshold of {f1_threshold:.2f}."
    assert accuracy >= accuracy_threshold, f"Accuracy {accuracy:.4f} is below the threshold of {accuracy_threshold:.2f}."

    # Step 5: Print metrics and success message
    print(f"Model performance passed all thresholds.")
    print(f"F1-score: {f1:.4f}, Accuracy: {accuracy:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Testing Expected F1-Score and Accuracy

# COMMAND ----------

model_uri

# COMMAND ----------

# Example usage of the test function
try:
    test_model_performance(
        model_uri=model_uri,
        X_test=X_test,
        y_test=y_test,
        f1_threshold=0.7,  # Custom threshold for F1-score
        accuracy_threshold=0.7  # Custom threshold for accuracy
    )
except AssertionError as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Testing Frameworks
# MAGIC
# MAGIC Unit tests are essential in the ML development process. They verify that individual components of your code. For example, it's common practice to check that functions or methods function as intended by data scientists and machine learning practitioners.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unittest
# MAGIC
# MAGIC unittest involves testing individual components of an ML pipeline to ensure correctness and reliability. Since machine learning pipelines consist of multiple stages—data preprocessing, feature engineering, model training, evaluation, and inference—unit testing helps validate the behavior of each component in isolation. Here we will consider unit tests for schema validation and checking missing values. 
# MAGIC
# MAGIC **Why use unittest?**
# MAGIC
# MAGIC Using unittest is better than writing individual functions for testing because it provides a structured, standardized, and scalable framework for managing and executing tests.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validating Schema and Missing and Non-negative Values
# MAGIC
# MAGIC Here we will return back to our first example and use unittest for data validation. Here, we will validate the schema, that no missing values are present, and non-negative values are not present.

# COMMAND ----------

import unittest
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, LongType
from pyspark.sql.functions import col, sum, min


class TestDataValidation(unittest.TestCase):
    """
    Unit tests for schema validation, missing values, and non-negative values in PySpark DataFrames.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up shared resources for the tests.
        """
        # Load the test DataFrame (assume a table named 'diabetes' is present)
        cls.df = spark.read.format("delta").table("diabetes").select(
            'id', 'Diabetes_binary', 'HighBP', 'BMI', 'Smoker', 'Stroke', 
            'HeartDiseaseorAttack', 'Age'
        )

    def test_validate_schema(self):
        """
        Test if the DataFrame schema matches the expected schema.
        """
        expected_schema = StructType([
            StructField("id", LongType(), True),
            StructField("Diabetes_binary", IntegerType(), True),
            StructField("HighBP", IntegerType(), True),
            StructField("BMI", IntegerType(), True),
            StructField("Smoker", IntegerType(), True),
            StructField("Stroke", IntegerType(), True),
            StructField("HeartDiseaseorAttack", IntegerType(), True),
            StructField("Age", DoubleType(), True),
        ])
        actual_schema = self.df.schema
        self.assertEqual(
            actual_schema, expected_schema,
            f"Schema validation failed.\nExpected: {expected_schema}\nActual: {actual_schema}"
        )

    def test_validate_no_missing_values(self):
        """
        Test that there are no missing (null) values in the DataFrame.
        """
        missing_values = self.df.agg(*[
            sum(col(c).isNull().cast("int")).alias(c) for c in self.df.columns
        ]).collect()[0].asDict()

        missing_columns = {col: missing_values[col] for col in self.df.columns if missing_values[col] > 0}
        self.assertFalse(
            missing_columns,
            f"Missing values found in the following columns: {missing_columns}"
        )

    def test_validate_non_negative_values(self):
        """
        Test that all columns in the DataFrame contain non-negative values (>= 0).
        """
        negative_values = self.df.agg(*[
            min(col(c)).alias(c) for c in self.df.columns
        ]).collect()[0].asDict()

        negative_columns = {col: negative_values[col] for col in self.df.columns if negative_values[col] < 0}
        self.assertFalse(
            negative_columns,
            f"Negative values found in the following columns: {negative_columns}"
        )


# Run the tests
suite = unittest.TestLoader().loadTestsFromTestCase(TestDataValidation)
unittest.TextTestRunner().run(suite)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that since we used our original DataFrame `df` that we are receiving the error about the negative value as intended!

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional Read) pytest
# MAGIC
# MAGIC Another testing framework that is popular for Python users is called pytest. pytest can be fully integrated into Databricks. You can read the documentation on implementing it within Databricks [here](https://docs.databricks.com/en/dev-tools/vscode-ext/pytest.html).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC In this demonstration, we looked at common testing strategies. We utilized MLflow and Unity Catalog for model training, Packaging, and registration. We also utilized Mosaic AI Model serving to showcase how to test for expected evaluation metrics and integrated endpoints. Finally, we looked at how unittest can be used in place of individual Python functions for testing machine learning pipelines.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
