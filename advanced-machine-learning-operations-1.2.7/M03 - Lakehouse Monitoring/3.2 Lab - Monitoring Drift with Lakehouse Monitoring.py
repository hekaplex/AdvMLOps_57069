# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab - Monitoring Drift with Lakehouse Monitoring
# MAGIC
# MAGIC In this lab, you will investigate how to monitor drift. You will work through a series of practical exercises to simulate prediction drift, set up monitoring for inference tables, and analyze drift metrics. The lab also includes hands-on tasks to explore visual dashboards, extract and modify SQL code, and create custom alerts. Additionally, you will learn to inspect lineage, enabling you to trace the flow of data and associated dashboards and queries.
# MAGIC
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC By the end of this lab, you will have completed the following:
# MAGIC - Initiate Lakehouse Monitoring programmatically
# MAGIC - Inspect automatically generated Dashboard
# MAGIC - Create a new widget in the monitoring dashboard
# MAGIC - Inspect Profile and Drift Metrics tables
# MAGIC - Setup an Alert

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
# MAGIC * To utilize the generated Dashboard, you will need a SQL Warehouse.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC To get into the lesson, we first need to build some data assets and define some configuration variables required for this lab. When running the following cell, the output is hidden so our space isn't cluttered. To view the details of the output, you can hover over the next cell and click the eye icon. 
# MAGIC
# MAGIC The cell after the setup, titled `View Setup Variables`, displays the various variables that were created. You can click the Catalog icon in the notebook space to the right to see that your catalog was created with no data.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-3.Lab

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initiate Inference Monitoring
# MAGIC
# MAGIC Here you will use Python to initiate the monitoring of your inference table using Lakehouse Monitoring. This will generate two table: profile metrics and drift metrics. In addition, a Databricks Dashboard will also be generated. We will inspect all these assets later. 
# MAGIC
# MAGIC Terminology: 
# MAGIC - Your **primary table** is your inference table you created by running the classroom setup. The name of this table is `model_logs`.
# MAGIC - Your **baseline table** is your source of truth for your data. The name of this table is `baseline_features`

# COMMAND ----------


from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorInferenceLog, MonitorInferenceLogProblemType, MonitorInfoStatus, MonitorRefreshInfoState, MonitorMetric
w = WorkspaceClient()
full_primary_table_name = <FILL_IN>
full_baseline_table_name = <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC During the initial classroom setup, we registered our inference table with Unity Catalog that contained features, predictions, and labels for a classification problem. Next, we will set essential variables that will be used for enabling Lakehouse Monitoring and initiate monitoring. 
# MAGIC
# MAGIC Configure the setup with the following conditions. 
# MAGIC 1. Set the window size to **1 day**
# MAGIC 1. Store the generated notebook in a folder in your homeworkspace with the path `<username>/My_Monitored_Dashboards/Drift_Detection`
# MAGIC 1. Set the slicer to include `Ages` between 3 and 10 and `HvyAlcoholConsump` to be 1 (non-alcoholic).

# COMMAND ----------


## ML problem type, either "classification" or "regression"
PROBLEM_TYPE = MonitorInferenceLogProblemType.<FILL_IN>

## Window sizes to analyze data over
GRANULARITIES = <FILL_IN>   

## Directory to store generated dashboard
ASSETS_DIR = f"/Workspace/Users/{DA.username}/databricks_lakehouse_monitoring/{primary_table_name}"

## Optional parameters
SLICING_EXPRS = <FILL_IN>   # Expressions to slice data with

# COMMAND ----------

# MAGIC %md
# MAGIC Run the initialization for Lakehouse Monitoring using the configuration above. Recall that for this dataset, we are interested in `Diabetes_binary` as our target variable.

# COMMAND ----------


print(f"Creating monitor for model_logs")
try: 
  info = w.quality_monitors.create(
    table_name=<FILL_IN>,
    inference_log=MonitorInferenceLog(
      timestamp_col='timestamp',
      granularities=<FILL_IN>,
      model_id_col='model_id',  ## Model version number 
      prediction_col=<FILL_IN>,  ## Ensure this column is of type DOUBLE
      problem_type=<FILL_IN>,
      label_col=<FILL_IN>  ## Ensure this column is of type DOUBLE
    ),
    baseline_table_name=<FILL_IN>,
    slicing_exprs=<FILL_IN>,
    output_schema_name=f"{DA.catalog_name}.{DA.schema_name}",
    assets_dir=<FILL_IN>
  )

  import time

  ## Wait for monitor to be created
  while info.status ==  MonitorInfoStatus.MONITOR_STATUS_PENDING:
    info = w.quality_monitors.get(table_name=<FILL_IN>)
    time.sleep(10)

  assert info.status == MonitorInfoStatus.MONITOR_STATUS_ACTIVE, "Error creating monitor"
except Exception as e:
  print(f"Error creating monitor: {e}")

# COMMAND ----------

## A metric refresh will automatically be triggered on creation
refreshes = w.quality_monitors.list_refreshes(table_name=<FILL_IN>).refreshes
assert(len(refreshes) > 0)

run_info = refreshes[0]
while run_info.state in (MonitorRefreshInfoState.PENDING, MonitorRefreshInfoState.RUNNING):
  run_info = w.quality_monitors.get_refresh(table_name=<FILL_IN>, refresh_id=run_info.refresh_id)
  print(run_info)
  time.sleep(30)

assert run_info.state == MonitorRefreshInfoState.SUCCESS, "Monitor refresh failed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Locate the Monitoring Dashboard
# MAGIC
# MAGIC There are two methods for locating the monitoring Dashboard.
# MAGIC
# MAGIC 1. Navigate to the underlying inference table `model_logs` and click on **Quality** and click on **View Dashboard**
# MAGIC 1. Navigate to the underlying inference table `model_logs` and click on **Lineage** and click on **Open in a dashboard** and find the dashboard you just created.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Monitoring Dashboard
# MAGIC
# MAGIC 1. After navigating to the canvas, scroll down until you see **Numerical Feature Drift**.
# MAGIC 1. Click on the kebab on the widget with title **# Features with Numerical Drift**.
# MAGIC 1. Click on **View Dataset**. This will take you to the generated **SQL code**. Modify it to not limit to the latest window by commenting out a single line.
# MAGIC 1. Copy and paste your answer in the next cell.
# MAGIC
# MAGIC **Do not run the SQL code** -  you will receive an error since you are working in a notebook. However, you can run the modified code in the Data tab of your dashboard.

# COMMAND ----------


## Copy and paste the code you modified above. 
<FILL_IN>

# COMMAND ----------

# MAGIC %md
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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a New Widget in the Canvas
# MAGIC
# MAGIC Next, create a new widget that provides a visual counter for the precision macro during the last window. 
# MAGIC
# MAGIC Steps: 
# MAGIC 1. Navigate to the canvas you just created. 
# MAGIC 1. Copy or create a new visualization using the **Add a visualization** blue button at the bottom of the Canvas screen. 
# MAGIC 1. Set the Dataset value to **performance_last_window**. 
# MAGIC 1. Set the **Visualization** to **Counter**.
# MAGIC 1. Set the **Value** to **precision_macro**. 
# MAGIC 1. Set the **Target** to **Window**.
# MAGIC 1. Give it a title **Precision Macro** as the **Title** and **In the last time window** for the **Description**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect Profile and Drift Metrics 
# MAGIC
# MAGIC Now we will turn our focus to inspecting the profile and drift metric tables that were created when we set up Lakehouse Monitoring. Let's read in both tables.

# COMMAND ----------


## Display drift metrics table
drift_table = f"{DA.catalog_name}.{DA.schema_name}.<FILL_IN>"
drift_table_df = spark.sql(<FILL_IN>)
display(drift_table_df.orderBy(F.rand()))

## Display profile metrics table
profile_table = f"{DA.catalog_name}.{DA.schema_name}.<FILL_IN>"
profile_table_df = spark.sql(<FILL_IN>)
display(drift_table_df.orderBy(F.rand()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use SQL to Analyze Profile Metrics Table

# COMMAND ----------

# MAGIC %md
# MAGIC Next, display the profile metrics table where the baseline data was logged and the f1-score is not null and order by accuracy_score.

# COMMAND ----------

## Display profile metrics table
profile_table = f"{DA.catalog_name}.{DA.schema_name}.model_logs_profile_metrics"
profile_table_df = spark.sql(f"SELECT * FROM {profile_table} where f1_score is not null and log_type = 'BASELINE' order by accuracy_score")
display(profile_table_df.orderBy(F.rand()))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use SQL to inspect the drift metrics
# MAGIC
# MAGIC Use SQL to inspect drift metrics by finding all records where
# MAGIC 1. The drift type is baseline
# MAGIC 1. No slice key
# MAGIC 1. BMI data only

# COMMAND ----------

# MAGIC %sql
# MAGIC <FILL_IN>

# COMMAND ----------

# MAGIC %md
# MAGIC Build custom code to inspect drift using KS test.

# COMMAND ----------


import pandas as pd
import json

## Load the drift metrics data from the Delta table
drift_table =<FILL_IN>
drift_metrics_df = <FILL_IN>

## Convert to Pandas DataFrame
data = drift_metrics_df.toPandas()

## Convert Timestamp objects to strings in 'window' and 'window_cmp'
def convert_timestamp_to_string(d):
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, pd.Timestamp):
                d[k] = v.isoformat()
            elif isinstance(v, dict):
                d[k] = convert_timestamp_to_string(v)
    return d

data['window'] = data['window'].apply(<FILL_IN>)
data['window_cmp'] = data['window_cmp'].apply(<FILL_IN>)

## Ensure JSON fields are strings
data['window'] = data['window'].apply(json.dumps)
data['window_cmp'] = data['window_cmp'].apply(json.dumps)

## Convert the JSON string in 'window' and 'window_cmp' to dictionaries
for index, row in data.iterrows():
    row['window'] = json.loads(row['window'])
    row['window_cmp'] = json.loads(row['window_cmp'])

## Analyze the drift metrics setting the threshold to 0.6
drift_thresholds = <FILL_IN>




def check_drift(row):
    ks_test_value = row['ks_test'].get('statistic') if isinstance(row['ks_test'], dict) else row['ks_test']
    if ks_test_value is not None and ks_test_value > drift_thresholds['ks_test']:
        return True
    return False

data['drift_detected'] = data.apply(<FILL_IN>, axis=1)

## Display rows with drift detected
drifted_rows = <FILL_IN>
no_drifted_rows = <FILL_IN>

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
# MAGIC    - Write a query to calculate the average value of the **Count** column in your profile metrics table:
# MAGIC      
# MAGIC       ```SELECT AVG(Count) AS avg_count FROM model_logs_profile_metrics;```
# MAGIC
# MAGIC    - Give the Query a name like `Drift Alert 1`
# MAGIC    - Once the query is complete, click **Save** to save it.
# MAGIC
# MAGIC 1. **Create an Alert**:
# MAGIC    - Navigate to **Alerts** on the left-hand sidebar and click **Create Alert**.
# MAGIC    - Provide the alert with a **name** like `Test alert`.
# MAGIC    - In the right-hand SQL editor panel, paste the query you have saved before.
# MAGIC > ****Note: Before running the query, make sure to set the current catalog and schema, and follow the `catalog.schema.table_name` structure.****
# MAGIC    - Click the **Run** button to validate the query.
# MAGIC    - Define a **Trigger Condition**:
# MAGIC      - Example: Trigger the alert when the value is above 20.
# MAGIC    - Click **Test condition**.
# MAGIC      - If the result satisfies the condition, you’ll see:`Triggered: Alert will trigger based on current data.`
# MAGIC    - Set the **Schedule**.
# MAGIC    - Under **Notify**, search and add users or destinations. 
# MAGIC    - Finally, click **Create** and select the folder to save it.
# MAGIC 1. **Monitor Lineage and Updates**:
# MAGIC    - Go back to the **Catalog** on the left sidebar.
# MAGIC    - Navigate to your **model_logs_profile_metrics** table.
# MAGIC    - Click on **Lineage** and then **Queries** to see the queries associated with this table.
# MAGIC       - You will see the `Drift Alert 1` listed there.
# MAGIC    - Whenever there’s an update or drift detected in your model or dataset, the alert will notify you automatically. You can then go to the table's **Lineage** tab to investigate the issue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this lab, you learned how to programmatically setup Lakehouse Monitoring on an inference table for detecting drift. This included investigating the automatically generated dashboard as well as the two metric tables: profile metrics and drift metrics. Additionally, you set up an alert use SQL to notify you when you suspect drifting may occur.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
