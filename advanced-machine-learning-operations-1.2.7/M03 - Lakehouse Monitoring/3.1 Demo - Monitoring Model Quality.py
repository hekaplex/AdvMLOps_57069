# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demonstration - Monitoring Model Quality
# MAGIC
# MAGIC In this demonstration, we will investigate how Databricks enables drift detection using various tools and features such as Notebooks, Queries, Dashboards, Alerts, while emphasizing Lakehouse Monitoring. Lakehouse Monitoring is leveraged to monitor the quality and statistical properties of data and ML models within the Databricks Lakehouse architecture. It allows you to track data quality, detect anomalies, and monitor the performance of ML models and model-serving endpoints.
# MAGIC
# MAGIC As a part of the classroom setup, we will bring in a pre-generated inference table (with Change Data Feed enabled) utilizing Databricks' Marketplace as well as a baseline dataset to help demonstrate how we can detect and investigate drift. In practice, it is recommended that Delta Lake's Change Data Feed (CDF), which allows Databricks to track row-level changes between versions of a Delta table, be enabled on inference tables by default. By leveraging CDF, data scientists can more efficiently create and monitor inference tables. 
# MAGIC
# MAGIC
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC
# MAGIC By the end of this demonstration, you will be able to do the following:
# MAGIC
# MAGIC - Understand how to apply Lakehouse Monitoring for detecting different types of drift. Here are the core concepts and features:
# MAGIC     - Automated Dashboards built to analyze an inference profile
# MAGIC     - Know various ways in which you can locate the dashboard (Workspace,  and Lineage and Quality in Catalog Explorer)
# MAGIC     - Know various ways in which you can create additional widgets in the dashboard
# MAGIC     - Automate profile and drift metric tables
# MAGIC     - Out-of-the-box metrics and custom metrics
# MAGIC     - Inspect inference profile lineage and create alerts for sending notifications when metrics change
# MAGIC     - Using Lakehouse Monitoring to build custom for analyzing possible drift. 
# MAGIC         - Review of various types of drift and Lakehouse Monitoring can be applied
# MAGIC - Understand additional configurations and features using the UI
# MAGIC     - How to Update a baseline feature table
# MAGIC     - Update slicing configurations
# MAGIC
# MAGIC **Note: This demonstration uses a mixture of SQL and Python.**

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
# MAGIC
# MAGIC * To utilize the generated Dashboard, you will need a SQL Warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC To get into the lesson, we first need to build some data assets and define some configuration variables required for this demonstration. When running the following cell, the output is hidden so our space isn't cluttered. To view the details of the output, you can hover over the next cell and click the eye icon. 
# MAGIC
# MAGIC The cell after the setup, titled `View Setup Variables`, displays the various variables that were created. You can click the Catalog icon in the notebook space to the right to see that your catalog was created with no data.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-3.1

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Dataset Location:  {DA.paths.datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference and Feature Tables
# MAGIC
# MAGIC As a part of the classroom setup, we have created two tables:
# MAGIC 1. `model_logs`: This is our inference table. It contains inference data from a sequence of queries that were made prior to this demo and will act as our dataset that is drifting away from the baseline data `(You can search for the dataset in the Marketplace by the name: Inference Dataset for Machine Learning Operations Associate Course.)`. 
# MAGIC 1. `baseline_features`: This is our baseline table and acts as our source of truth.
# MAGIC
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC **A Remark on Inference Tables and Inference Profiles**
# MAGIC
# MAGIC It's important to distinguish between an inference table when thinking about the assets we will be working with. An inference table can be thought of as a collection of logs from the model that contain the input features at runtime and predicted labels. You can also include the true labels if they are available. On the other hand, an inference profile is an inference table that also contains profile metrics like F1-score, accuracy, or other evaluation metrics that measure the performance of the model on the inferred data.

# COMMAND ----------

primary_table_name = 'model_logs'
baseline_table_name = 'baseline_features'

primary_df = spark.read.format('delta').table(primary_table_name)
primary_pdf = primary_df.toPandas()

baseline_df = spark.read.format('delta').table(baseline_table_name)
baseline_pdf = baseline_df.toPandas()


inference_df = spark.read.format('delta').table('model_inference_table')



display(baseline_df)
display(primary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Motivating Drift Investigation
# MAGIC We'll take a cross-sectional snapshot of the distributions of BMI and Diabete_binary data to compare the ground truth to the data in our inference table.

# COMMAND ----------

# DBTITLE 1,BMI Distribution Snapshot
# Compare distributions for a feature
import pandas as pd
import scipy.stats as stats
import matplotlib.pyplot as plt

#Randomly sample from the pandas dataframe baseline_pdf with samples equal to the size of primary_pdf
baseline_pdf_sample = baseline_pdf.sample(n=len(primary_pdf), random_state=1)


# KS test
stat, p_value = stats.ks_2samp(baseline_pdf_sample['BMI'], primary_pdf['BMI'])

# Visualization 1
plt.hist(baseline_pdf_sample['BMI'], bins=30, alpha=0.5, label="Training Data")
plt.hist(primary_pdf['BMI'], bins=30, alpha=0.5, label="Logged Data")
plt.legend()
plt.title("BMI Distribution: Training vs Logged (Drift Detected)")
plt.show()

# Visualization 2
plt.hist(baseline_pdf_sample['labeled_data'], bins=30, alpha=0.5, label="Training Data")
plt.hist(primary_pdf['Diabetes_binary'], bins=30, alpha=0.5, label="Predicted Data")
plt.legend()
plt.title("Target Distribution: Training vs Logged (Drift Detected)")
plt.show()

print(f'KS Test statistic value: {stat} and p-value: {p_value}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakehouse Monitoring
# MAGIC
# MAGIC To monitor the performance of the model we used to build our premade `model_logs` table, we will attach the monitor to this table. Our baseline table `baseline_features` is what we can consider as our *good* or *source of truth* data. The Databricks monitor creates out-of-the-box metrics that are important and sets up two tables: 
# MAGIC
# MAGIC 1. Profile metrics table: This keeps track of profile metrics, which is a summary statistic computed for each column. 
# MAGIC 1. Drift metrics table:  This keeps track of changes in profile metrics compared to baseline or previous time window. 
# MAGIC
# MAGIC These two tables are used to automatically generate a monitoring dashboard. Later, we will uses these tables to build an alert for drift.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1: Using the UI
# MAGIC
# MAGIC Let's first start with recalling how we can use the UI to setup with Lakehouse Monitoring
# MAGIC
# MAGIC To add a monitor on the inference table, 
# MAGIC
# MAGIC 1. Open the **Catalog** menu from the left menu bar.
# MAGIC
# MAGIC 1. Select the table **model_logs** within your catalog and schema. 
# MAGIC
# MAGIC 1. Click on the **Quality** tab then on the **Get started** button.
# MAGIC
# MAGIC 1. As **Profile type** select **Inference profile**.
# MAGIC
# MAGIC 1. As **Problem type** select **classification**.
# MAGIC
# MAGIC 1. As the **Prediction column** select **Diabetes_binary**.
# MAGIC
# MAGIC 1. As the **Label column** select **labeled_data**
# MAGIC
# MAGIC 1. As **Metric granularities** select **5 minutes**, **1 hour**, and **1 day**. We will use the doctored timestamps to simulate requests that have been received over a large period of time. 
# MAGIC
# MAGIC 1. As **Timestamp column** select **timestamp**.
# MAGIC
# MAGIC 1. As **Model ID column** select **model_id**.
# MAGIC
# MAGIC 1. In Advanced Options --> **Unity catalog baseline table name (optional)** enter **Your baseline table name** from above along with your catalog and schema(Eg: catalog.schema.table_name).
# MAGIC
# MAGIC 1. Click the **Create** button.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Pythonic Approach
# MAGIC For a more programmatic approach, you can run the following code to initiate Lakehouse Monitoring. This method allows you to automate and customize the monitoring according to specific needs and thresholds.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorInferenceLogProblemType, MonitorInfoStatus, MonitorRefreshInfoState, MonitorMetric
w = WorkspaceClient()
full_primary_table_name = f'{DA.catalog_name}.{DA.schema_name}.{primary_table_name}'
full_baseline_table_name = f"{DA.catalog_name}.{DA.schema_name}.baseline_features"

# COMMAND ----------

help(w.quality_monitors.create)

# COMMAND ----------

#ML problem type, either "classification" or "regression"
PROBLEM_TYPE = MonitorInferenceLogProblemType.PROBLEM_TYPE_CLASSIFICATION

# Window sizes to analyze data over
GRANULARITIES = ["1 day"]   

# Directory to store generated dashboard
ASSETS_DIR = f"/Workspace/Users/{DA.username}/databricks_lakehouse_monitoring/{primary_table_name}"

# Optional parameters
SLICING_EXPRS = ["Age < 2", "Age > 15", "Sex = 1", "HighChol = 1"]   # Expressions to slice data with

# COMMAND ----------

print(f"Creating monitor for model_logs")
try: 
  info = w.quality_monitors.create(
    table_name=full_primary_table_name,
    inference_log=MonitorInferenceLog(
      timestamp_col='timestamp',
      granularities=GRANULARITIES,
      model_id_col='model_id',  # Model version number 
      prediction_col='Diabetes_binary',  # Ensure this column is of type DOUBLE
      problem_type=PROBLEM_TYPE,
      label_col='labeled_data'  # Ensure this column is of type DOUBLE
    ),
    baseline_table_name=full_baseline_table_name,
    slicing_exprs=SLICING_EXPRS,
    output_schema_name=f"{DA.catalog_name}.{DA.schema_name}",
    assets_dir=ASSETS_DIR
  )

  import time

  # Wait for monitor to be created
  while info.status ==  MonitorInfoStatus.MONITOR_STATUS_PENDING:
    info = w.quality_monitors.get(table_name=full_primary_table_name)
    time.sleep(10)

  assert info.status == MonitorInfoStatus.MONITOR_STATUS_ACTIVE, "Error creating monitor"
except Exception as e:
  print(f"Error creating monitor: {e}")

# COMMAND ----------

# A metric refresh will automatically be triggered on creation
refreshes = w.quality_monitors.list_refreshes(table_name=full_primary_table_name).refreshes
assert(len(refreshes) > 0)

run_info = refreshes[0]
while run_info.state in (MonitorRefreshInfoState.PENDING, MonitorRefreshInfoState.RUNNING):
  run_info = w.quality_monitors.get_refresh(table_name=full_primary_table_name, refresh_id=run_info.refresh_id)
  print(run_info)
  time.sleep(30)

assert run_info.state == MonitorRefreshInfoState.SUCCESS, "Monitor refresh failed"

# COMMAND ----------

w.quality_monitors.get(table_name=full_primary_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the Lakehouse Monitoring Dashboard
# MAGIC
# MAGIC Methods for accessing the dashboard:
# MAGIC 1. Run the next cell and click on the generated link. This will take you to the `model_logs` in the Catalog Explorer.
# MAGIC 1. Select Quality and click on View Dashboard.  
# MAGIC 1. The Dashboard (and other assets) are automatically registered to the lineage of `model_logs` as a part of comprehensive and automated tracking with Unity Catalog. 
# MAGIC 1. Setting the `ASSETS_DIR` gives the location for where the generated dashboards are actually stored. In this demo, this is set to `f"/Workspace/Users/{DA.username}/databricks_lakehouse_monitoring/{primary_table_name}"`. 
# MAGIC
# MAGIC The dashboard provides a comprehensive view of the data quality and statistical properties of the tables being monitored. It helps in tracking data integrity, statistical distribution, and data drift over time. This is where you will prebuilt widgets that incorporate the metrics generated from the profile and drift metric tables. The dashboard should be thought of as a starting point for monitoring for drift, hence it is fully customizable. 
# MAGIC
# MAGIC - At the top left you can toggle between viewing the **Canvas** or the underlying **Data** that was generated with dashboard using SQL logic. Note that if a widget is currently being used, then the dataset created cannot be deleted. 
# MAGIC - Select widget that reads # Inferences on the left side of the canvas
# MAGIC     - Click on the kebab and select View Dataset. This will take you to the generated SQL code that you can use for further analysis with the SQL Editor or in this notebook.
# MAGIC     - Try editing the code and clicking Run to see how you can analyze the underlying tables generated as a part of the Dashboard creation.
# MAGIC - Back in the canvas, notice some metrics may appear as `null` or indicate **no data available**. This is expected since we are working with a sample dataset, since the automatically generated SQL code has logic that might not agree with this dataset such as comparing only baseline features or compute metrics on categorical features, which we are not considering here. If a baseline table was provided, drift metrics will be monitored. 
# MAGIC - For example, scroll down to the **Numerical Feature Drift** section and inspect the code for **# Features with Numerical Drift**. Notice that this SQL code only investigates the last window from the drift table. 
# MAGIC

# COMMAND ----------

# Extract workspace URL
workspace_url = spark.conf.get('spark.databricks.workspaceUrl')

# Construct the monitor dashboard URL
monitor_dashboard_url = f"https://{workspace_url}/explore/data/{DA.catalog_name}/{DA.schema_name}/model_logs?o={DA.schema_name}&activeTab=quality"

print(f"Monitor Dashboard URL: {monitor_dashboard_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC > **⚠️ Note:**  
# MAGIC > You may encounter a `CAST_INVALID_INPUT` error in the **Numerical Feature Quantile Drift** widget.  
# MAGIC > This occurs because the auto-generated query attempts to cast the string `"Baseline"` to a `TIMESTAMP` type in the `Window` column.  
# MAGIC > While this error does not affect other metrics or drift detection functionality, you can optionally resolve it using the steps below.
# MAGIC
# MAGIC **Steps to Diagnose and Fix the Error (Optional)**
# MAGIC - **Step 1:**  
# MAGIC   Navigate to the **Numerical Feature Quantile Drift** widget in the monitoring dashboard.  
# MAGIC   If there's a rendering failure, you will see a `CAST_INVALID_INPUT` error message displayed within the widget.
# MAGIC - **Step 2:**  
# MAGIC   Click the **kebab menu (⋮)** in the top-right corner of the widget and select **View Dataset**.  
# MAGIC   This opens the SQL query that powers the visual.
# MAGIC - **Step 3:**  
# MAGIC   In the dataset editor, scroll down and click **Diagnose error**.  
# MAGIC   The Databricks Assistant will analyze the issue and provide a suggested fix.
# MAGIC - **Step 4:**  
# MAGIC   Click **Replace active cell content** to apply the Assistant’s suggested query.  
# MAGIC   The corrected query will ensure the `Window` column in the baseline portion uses a proper timestamp type, typically by replacing `"Baseline"` with `CAST(NULL AS TIMESTAMP)`.
# MAGIC - **Step 5:**  
# MAGIC   Click **Run** to execute the updated query.
# MAGIC - **Step 6:**  
# MAGIC   Once the query runs successfully, the widget should render correctly, and the error message will disappear.
# MAGIC This fix helps ensure type consistency in the `UNION` clause across datasets, resolving the rendering error in the visual.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a New Visual
# MAGIC
# MAGIC ### Clone Method
# MAGIC
# MAGIC - Click the kebab on the **Inference Load Over Time**
# MAGIC - Click **Clone**
# MAGIC - Click on the new visual
# MAGIC     - Notice the **Widget** pane has been automatically populated with the same configuration as the cloned visual.
# MAGIC - Under Visualization on the widget configuration menu to the right, select **Counter** from the dropdown menu. 
# MAGIC - Click on the dropdown menu for the Value and select **Count** under Field and select **SUM** under **Transform**
# MAGIC - Click on the dropdown menu under **Target** and select Window under Field and select **COUNT DISTINCT**
# MAGIC - Double click on the title to change the title to **# Inferences and Intervals Monitored**. 
# MAGIC
# MAGIC You will see a new counter displayed now that shows the total inference load over time and how many distinct windows there are. You can also resize the visual by dragging the corner inward or outward. 
# MAGIC
# MAGIC - Select the kebab again
# MAGIC - Here you can download the data for this visualization and copy a link to the widget if you would like to share it with your team. 
# MAGIC
# MAGIC ### Custom Visual
# MAGIC
# MAGIC Additionally, you build your own visual.
# MAGIC - At the bottom of the canvas, there is a blue box that contains **Select** **Add a visualization**, **Add a textbox**, and **Add a filter**. Select **Add a visualization**. 
# MAGIC - In the visualization editor on the right of your screen:
# MAGIC         - Select **profile_over_time** under **Dataset**
# MAGIC         - Select **Counter** under **Visualization**
# MAGIC         - Select **f1_score** under **Field** and **COUNT** under **Transform**
# MAGIC         - Select **Title** under **Widget** and give it title **# F1-Scores Over Time**
# MAGIC - Finally, inspect the SQL generated for this widget. 
# MAGIC
# MAGIC ### Additional Customization
# MAGIC
# MAGIC You can also build custom SQL code to build an even more customized visual by select **Create from SQL** at the very bottom of the Datasets pane to the left in the **Data** tab. Note this requires a detailed understanding of the profile and drift metrics tables and your specific goals for creating the new dataset(s).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore the 2 Metric Tables
# MAGIC
# MAGIC Now that we have inspected the automatically generated dashboard, let's take a look at the two Delta tables that built this dashboard. 
# MAGIC
# MAGIC 1. **Navigate to the Catalog Explorer**:
# MAGIC    - On the left-hand side of the Databricks workspace, click on **Catalog Explorer**.
# MAGIC
# MAGIC 1. **Locate Your Catalog**:
# MAGIC    - Find `dbacademy` catalog and select the `schema` that you are currently working in.
# MAGIC
# MAGIC 1. **Explore the New Tables**:
# MAGIC    - Look for two new tables:
# MAGIC      - `model_logs_drift_metrics`
# MAGIC      - `model_logs_profile_metrics`
# MAGIC    - Click on each table to explore the metrics that were calculated.
# MAGIC
# MAGIC 1. **Note on Metrics**:
# MAGIC    - Similar to the **Model Logs Dashboard**, some metrics may appear as `null` depending on the column's characteristics or data availability. For example, we have no categorical features within this dataset.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A Closer Look at Out-Of-The-Box Metrics
# MAGIC
# MAGIC The profile metrics table and drift metrics table created by Databricks Lakehouse Monitoring contain different types of information to help monitor and analyze data.
# MAGIC
# MAGIC ### Profile Metrics Table:
# MAGIC 1. **Summary Statistics:**  
# MAGIC    Contains summary statistics for each column, such as count, number of null values, distinct count, minimum, maximum, average, standard deviation, and more.
# MAGIC
# MAGIC 2. **Time Window and Slices:**  
# MAGIC    Metrics are computed for specified time intervals (windows) and for each data slice defined by slicing expressions.
# MAGIC
# MAGIC 3. **Grouping Columns:**  
# MAGIC    Includes columns like time window, granularity, log type (input or baseline table), slice key and value, and model ID (for InferenceLog analysis).
# MAGIC
# MAGIC 4. **Model Accuracy Metrics:**  
# MAGIC    For InferenceLog analysis, it includes metrics like accuracy score, confusion matrix, precision, recall, and F1 score.
# MAGIC
# MAGIC 5. **Fairness and Bias Statistics:**  
# MAGIC    For classification models, it includes metrics like predictive parity, predictive equality, equal opportunity, and statistical parity.
# MAGIC
# MAGIC ### Drift Metrics Table:
# MAGIC 1. **Drift Statistics:**  
# MAGIC    Tracks changes in distribution for a metric, comparing either to a previous time window (consecutive drift) or to a baseline distribution (baseline drift).
# MAGIC
# MAGIC 2. **Comparison Windows:**  
# MAGIC    Includes columns for the current and comparison time windows.
# MAGIC
# MAGIC 3. **Drift Types:**  
# MAGIC    Indicates whether the drift is compared to the previous window or a baseline table.
# MAGIC
# MAGIC 4. **Metrics Columns:**  
# MAGIC    Includes differences in count, average, percent null, percent zeros, percent distinct, and other statistical tests like:
# MAGIC    - Chi-squared test
# MAGIC    - KS test
# MAGIC    - Total variation distance
# MAGIC    - L-infinity distance
# MAGIC    - Jensen-Shannon distance
# MAGIC    - Wasserstein distance
# MAGIC    - Population stability index

# COMMAND ----------

# DBTITLE 1,Inspect Profile Metrics Table
# Display profile metrics table
profile_table = f"{DA.catalog_name}.{DA.schema_name}.model_logs_profile_metrics"
profile_df = spark.sql(f"SELECT * FROM {profile_table}")
display(profile_df.orderBy(F.rand()))

# COMMAND ----------

# DBTITLE 1,Inspect Drift Metrics Table
# Display profile metrics table
drift_table = f"{DA.catalog_name}.{DA.schema_name}.model_logs_drift_metrics"
drift_table_df = spark.sql(f"SELECT * FROM {drift_table}")
display(drift_table_df.orderBy(F.rand()))

# COMMAND ----------

# MAGIC %md
# MAGIC Let's inspect the profile metrics table and the drift metrics table. Notice that the value of a cell will read `Null` when the metric does not apply.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Utilizing Profile Metrics

# COMMAND ----------

# DBTITLE 1,Inspect Fairness and Bias Results
fb_cols = ["window", "model_id", "slice_key", "slice_value", "predictive_parity", "predictive_equality", "equal_opportunity", "statistical_parity"]
fb_metrics_df = profile_df.select(fb_cols).filter(f"column_name = ':table' AND slice_value = 'true'")
display(fb_metrics_df.orderBy(F.rand()).limit(10))

# COMMAND ----------

# DBTITLE 1,Inspect F1-Scores and Accuracy
import pandas as pd
import json

# Load the profile metrics data from the Delta table
profile_table = f"{DA.catalog_name}.{DA.schema_name}.model_logs_profile_metrics"
profile_metrics_df = spark.read.table(profile_table)

# Convert to Pandas DataFrame
data = profile_metrics_df.toPandas()

# Extract and format 'window' column (start and end timestamps)
def process_window_column(row):
    if isinstance(row, dict):
        return {
            "start": pd.to_datetime(row.get("start", None)),
            "end": pd.to_datetime(row.get("end", None))
        }
    elif isinstance(row, str):  # If the row is still serialized as a string
        try:
            row_dict = json.loads(row)
            return process_window_column(row_dict)
        except Exception:
            return {"start": None, "end": None}
    else:
        return {"start": None, "end": None}

# Apply the processing function to the 'window' column
data['window'] = data['window'].apply(process_window_column)

# Extract start and end into separate columns for clarity
data['window_start'] = data['window'].apply(lambda x: x['start'])
data['window_end'] = data['window'].apply(lambda x: x['end'])

# Drop rows with null or empty F1-scores
data = data.dropna(subset=['f1_score'])
data = data.dropna(subset =['accuracy_score'])

# Extract relevant metrics
# Replace 'f1_score' with the actual column name for F1-score in your table
f1_scores = data[['window_start', 'window_end', 'f1_score', 'accuracy_score']]

# Display F1-scores over time
print("F1-scores over time (non-empty values):")
display(f1_scores)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Utilizing Drift Metrics

# COMMAND ----------

# DBTITLE 1,Inspect results for BMI based on baseline features
# MAGIC %sql
# MAGIC select * from model_logs_drift_metrics
# MAGIC where drift_type = 'BASELINE' AND slice_key is null and column_name = 'BMI'

# COMMAND ----------

# DBTITLE 1,Inspect  KS test from Drift Metrics table
import pandas as pd
import json

# Load the drift metrics data from the Delta table
drift_table = f"{DA.catalog_name}.{DA.schema_name}.model_logs_drift_metrics"
drift_metrics_df = spark.read.table(drift_table)

# Convert to Pandas DataFrame
data = drift_metrics_df.toPandas()

# Convert Timestamp objects to strings in 'window' and 'window_cmp'
def convert_timestamp_to_string(d):
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, pd.Timestamp):
                d[k] = v.isoformat()
            elif isinstance(v, dict):
                d[k] = convert_timestamp_to_string(v)
    return d

data['window'] = data['window'].apply(convert_timestamp_to_string)
data['window_cmp'] = data['window_cmp'].apply(convert_timestamp_to_string)

# Ensure JSON fields are strings
data['window'] = data['window'].apply(json.dumps)
data['window_cmp'] = data['window_cmp'].apply(json.dumps)

# Convert the JSON string in 'window' and 'window_cmp' to dictionaries
for index, row in data.iterrows():
    row['window'] = json.loads(row['window'])
    row['window_cmp'] = json.loads(row['window_cmp'])

# Analyze the drift metrics
drift_thresholds = {
    "ks_test": 0.6,
}




def check_drift(row):
    ks_test_value = row['ks_test'].get('statistic') if isinstance(row['ks_test'], dict) else row['ks_test']
    if ks_test_value is not None and ks_test_value > drift_thresholds['ks_test']:
        return True
    return False

data['drift_detected'] = data.apply(check_drift, axis=1)

# Display rows with drift detected
drifted_rows = data[data['drift_detected']]
no_drifted_rows = data[~data['drift_detected']]

print("Rows with drift detected:")
display(drifted_rows)

print("\nRows with no drift detected:")
display(no_drifted_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Lineage and Alerts for Drift Detection
# MAGIC
# MAGIC The following instructions are not meant to send you an alert to any device. It is only to demonstrate how to use lineage alongside alerts for your metric tables.
# MAGIC
# MAGIC 1. **Navigate to Queries**:
# MAGIC    - On the left-hand sidebar of the Databricks workspace, click on **Queries**.
# MAGIC
# MAGIC 1. **Create a Query**:
# MAGIC    - Click on **Create Query** in the top-right corner.
# MAGIC    - Write a query to calculate the maximum value of the median BMI value (this median value is computed per window):
# MAGIC      
# MAGIC          select
# MAGIC          max(median) as largest_median
# MAGIC          FROM model_logs_profile_metrics
# MAGIC          WHERE column_name = 'BMI'
# MAGIC          GROUP BY column_name;
# MAGIC    - Give the Query a name like `Drift Alert 1`
# MAGIC    - Once the query is complete, click **Save** to save it.
# MAGIC
# MAGIC 1. **Create an Alert**:
# MAGIC    - Navigate to **Alerts** on the left-hand sidebar and click **Create Alert**.
# MAGIC    - Provide the alert with a **name** like `Test alert`.
# MAGIC    - In the right-hand SQL editor panel, paste the query you have saved before.
# MAGIC > ****Note: Before running the query, make sure to set the current catalog and schema, and follow the `catalog.schema.table_name` structure.****
# MAGIC
# MAGIC    - Click the **Run** button to validate the query.
# MAGIC    - Define a **Trigger Condition**:
# MAGIC      - Example: Trigger the alert when the value is above 50 (50 is the max).
# MAGIC    - Click **Test condition**.
# MAGIC      - If the result satisfies the condition, you’ll see:`Triggered: Alert will trigger based on current data.`
# MAGIC    - Set the **Schedule**.
# MAGIC    - Under **Notify**, search and add users or destinations. 
# MAGIC    - Finally, click **Create** and select the folder to save it.
# MAGIC ![alert](../Includes/images/alert.png)
# MAGIC
# MAGIC 1. **Monitor Lineage and Updates**:
# MAGIC    - Go back to the **Catalog** on the left sidebar.
# MAGIC    - Navigate to your **model_logs_profile_metrics** table.
# MAGIC    - Click on **Lineage** and then **Queries** to see the queries associated with this table.
# MAGIC       - You will see the `Drift Alert 1` listed there.
# MAGIC    - Whenever there’s an update or drift detected in your model or dataset, the alert will notify you automatically. You can then go to the table's **Lineage** tab to investigate the issue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Profile and Drift Metrics for Other Types of Drift
# MAGIC If we wish to monitor other types of drift (see below), then we can leverage the drift metrics table to do so.
# MAGIC
# MAGIC Here we outline the metrics to monitor **concept drift**, **model quality drift**, and **bias drift**, along with guidance on their application.
# MAGIC
# MAGIC | **Drift Type**      | **Definition**                                                                                                                                                                 | **Applicable Metrics**                                                                                                                                                                                                                                                                                        | **Additional Metrics**                                                                                                                                                                                                                              | **How to Use**                                                                                                                                                                                                                 |
# MAGIC |----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
# MAGIC | **Concept Drift**    | Changes in the relationship between input features and the target variable.                                                                                                 | - **KS Test (`ks_test`)**: Evaluate numeric feature drift.<br>- **Chi-Square Test (`chi_squared_test`)**: Analyze categorical feature drift.<br>- **TV Distance (`tv_distance`)**: Measure distributional differences.<br>- **Wasserstein Distance (`wasserstein_distance`)**: Capture numeric feature changes.<br>- **JS Distance (`js_distance`)**: Measure categorical similarity. | - **Predicted Probabilities (`pred_prob_delta`)**: Check prediction score changes.<br>- **Residual Distribution (`residual_delta`)**: Analyze residual distribution shifts over time.                                                               | - Compare feature distributions from training data to current data.<br>- Check conditional distributions of features given the target.<br>- Monitor predicted probabilities and residuals for deviations over time.                                   |
# MAGIC | **Model Quality Drift** | Degradation in the model's predictive performance over time.                                                                                                               | - **Count Delta (`count_delta`)**: Detect changes in data volume.<br>- **Avg Delta (`avg_delta`)**: Highlight feature mean shifts.<br>- **Percent Null Delta (`percent_null_delta`)**: Track changes in missing data rates.<br>- **Non-Null Columns Delta (`non_null_columns_delta`)**: Identify missing or new features. | - **Performance Metrics**: Monitor metrics like accuracy, RMSE, AUC, or F1 score.<br>- **Feature Importance Drift**: Observe changes in feature importance.<br>- **Residual Metrics**: Track residual trends (e.g., mean, variance).                  | - Monitor metrics for deviations from expected ranges.<br>- Use proxy metrics and performance metrics to assess drift.<br>- Address significant deviations with retraining or adjustments.                                                           |
# MAGIC | **Bias Drift**       | Changes that affect the fairness or equitable performance of the model across subgroups.                                                                                    | - **Chi-Square Test (`chi_squared_test`)**: Detect subgroup distributional shifts.<br>- **JS Distance (`js_distance`)**: Measure subgroup similarity changes.<br>- **Population Stability Index (`population_stability_index`)**: Track population shifts for subgroups.                                         | - **Outcome-Based Fairness Metrics**:<br> - **Disparate Impact**: Ratio of positive outcomes between subgroups.<br> - **Equalized Odds**: Differences in false positive/negative rates between subgroups.<br> - **Demographic Parity**: Positive prediction rates comparison.<br>- **Performance Metrics by Subgroup**: Assess subgroup-specific performance metrics (e.g., accuracy, recall). | - Monitor subgroup feature distributions and outcomes.<br>- Address fairness concerns with outcome-based metrics.<br>- Use metrics to identify and address bias comprehensively.                                                                    |
# MAGIC
# MAGIC ### **Overall Guidance**
# MAGIC - Use these metrics alongside domain knowledge and performance evaluations.
# MAGIC - Contextual interpretation is critical to avoid false positives or overcorrecting.
# MAGIC - Combine proxy metrics, qualitative checks, and direct performance metrics for a comprehensive drift monitoring framework.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Additional Configurations
# MAGIC
# MAGIC There are other types of advanced configurations you setup. Although, because this monitor was previously created, you will have to refresh the process by clicking on **Refresh Metrics** under **Quality** for the **model_logs** table. We provide a few below as a part of this demonstration.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Custom Metric Creation: Aggregate metric (using the UI)
# MAGIC
# MAGIC On top of the analysis and drift statistics that are automatically generated with Lakehouse Monitoring, you can create custom metrics as well. 
# MAGIC
# MAGIC There are 3 types of custom metrics that can be used with Lakehouse Monitoring:
# MAGIC 1. Aggregated metrics
# MAGIC 1. Derived metrics
# MAGIC 1. Drift metrics
# MAGIC
# MAGIC
# MAGIC To create and implement a custom metric in Databricks Lakehouse Monitoring, follow these steps:
# MAGIC
# MAGIC 1. **Access the Catalog**
# MAGIC    - Navigate to the **Catalog** from the left sidebar menu.
# MAGIC
# MAGIC 1. **Open Model Logs**
# MAGIC    - In the Catalog, locate and click on **model_logs**.
# MAGIC
# MAGIC 1. **Edit Monitor Configuration**
# MAGIC    - Click on the **Quality** tab.
# MAGIC    - Select **Edit Monitor Configuration** (look for the gear icon next to it).
# MAGIC
# MAGIC 1. **Enable Advanced Options**
# MAGIC    - Scroll down to **Advanced Options** and expand the drop-down menu.
# MAGIC
# MAGIC 1. **Add Custom Metric**
# MAGIC    - Select **Add Custom Metric** from the menu.
# MAGIC
# MAGIC 1. **Define the Custom Metric**
# MAGIC    - Enter the following details for your custom metric:
# MAGIC      - **Name**: `AverageDifferenceFruitsVeggies`
# MAGIC      - **Type**: `Aggregate`
# MAGIC      - **Input Columns**: `Fruits`, `Veggies`
# MAGIC      - **Output Type**: `Double`
# MAGIC      - **Definition**:
# MAGIC        AVG(Fruits - Veggies)
# MAGIC
# MAGIC 1. **Do NOT Save and Apply**
# MAGIC    - Saving the configuration and reloading the table will take quite a bit of time. 
# MAGIC
# MAGIC 1. **Refresh Monitor**
# MAGIC    - Once the metric is added, refresh the monitor to compute and view the results.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC An example of how to configure a custom metric this with Python (and SQL Jinja template) is provided here:
# MAGIC
# MAGIC ```
# MAGIC from databricks.sdk.service.catalog import MonitorMetric, MonitorMetricType
# MAGIC from pyspark.sql import types as T
# MAGIC
# MAGIC custom_metric = MonitorMetric(
# MAGIC     type=MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
# MAGIC     name="avg_diff_f1_f2",
# MAGIC     input_columns=[":table"],
# MAGIC     definition="avg(Fruits - Veggies)",
# MAGIC     output_data_type=T.StructField("output", T.DoubleType()).json(),
# MAGIC )
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC To read more on a more programmatic approach, please read this [official documentation](https://docs.databricks.com/en/lakehouse-monitoring/custom-metrics.html).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this demonstration, we discussed Lakehouse Monitoring and took a deep dive into how to monitor various metrics using Dashboards. You learned about out-of-the-box metrics as well as how to create a custom metric with the UI and programmatically. We also discussed how **Lineage** with Delta tables and **Alerts** can be used within Databricks for sending updates for when drift does occur with your profile and drift metric tables. Finally, you learned about how to set advanced configurations with your inference table such as adding slicers and granularities.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
