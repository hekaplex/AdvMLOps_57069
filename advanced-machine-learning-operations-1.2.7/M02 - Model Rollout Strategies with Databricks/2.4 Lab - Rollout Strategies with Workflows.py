# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Rollout Strategies with Workflows
# MAGIC
# MAGIC In this lab, you will do the following:
# MAGIC 1. Bring in a table from Databricks Marketplace.
# MAGIC 1. Perform feature engineering and store the table in Databricks Feature Store.
# MAGIC 1. Perform validation test to check for missing values, confirm the schema, and check for non-negative values. 
# MAGIC 1. You will also check that normalization was carried out correctly. 
# MAGIC 1. Finally, you will train and serve 2 models while directing traffic to one model 40% of the time and to the other model 60% of the time. 
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC
# MAGIC By the end of this lab you will be able to
# MAGIC - Demonstrate knowledge of how to setup integration tests with Databricks Workflows.
# MAGIC - Demonstrate knowledge of how to configure a model serving endpoint for serving an uneven traffic distribution for 2 models. 
# MAGIC - Demonstrate an understanding of working with MLflow and Unity Catalog for testing rollout strategies.

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
# MAGIC To get into the lesson, you first need to build some data assets and define some configuration variables required for this demonstration. When running the following cell, the output is hidden so our space isn't cluttered. To view the details of the output, you can hover over the next cell and click the eye icon. 
# MAGIC
# MAGIC The cell after the setup, titled `View Setup Variables`, displays the various variables that were created. You can click the Catalog icon in the notebook space to the right to see that your catalog was created with no data.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-2.Lab

# COMMAND ----------

# MAGIC %md
# MAGIC The following variables will be useful for you to configure your various parameters.

# COMMAND ----------

print(f"Username:          {DA.username}")
print(f"Catalog Name:      {DA.catalog_name}")
print(f"Schema Name:       {DA.schema_name}")
print(f"Working Directory: {DA.paths.working_dir}")
print(f"Dataset Location:  {DA.paths.datasets}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Workflow Setup
# MAGIC Here you will setup an integration test using Databrick Workflows. There will be a total of 5 Workflow notebooks that you will need to **fill out** as a part of completing this lab. They are located in the folder title **2.4 Lab - Model Training Pipeline**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Review of How to Attach Workflow Notebooks
# MAGIC
# MAGIC > **For Experienced Users:** If you are already familiar with setting up workflows, you can skip the detailed steps below and proceed directly to setting up the workflow.
# MAGIC > 
# MAGIC > **For New Users or Those Needing a Refresher:** Follow the step-by-step instructions below to set up your workflow.
# MAGIC
# MAGIC
# MAGIC 1. **Navigate to Jobs & Pipelines**:
# MAGIC    - Open the [**Jobs & Pipelines**](/jobs) page (also accessible from the left sidebar).
# MAGIC 1. Click on **Create** and select **Job** from the dropdown in the upper-right corner of the page.
# MAGIC 1. Set the Job name to ***Rollout Strategies with Workflow*** or something similar for easy identification.
# MAGIC
# MAGIC 1. **Add a New Task**:
# MAGIC    - Click on the **Tasks** tab.
# MAGIC    - In the middle of the screen, locate the blue button labeled **Add Task** with a plus sign next to it and click on it.
# MAGIC
# MAGIC 1. **Set Up the Task**:
# MAGIC    - Select **Notebook** at the top of the menu that appears.
# MAGIC    - Provide a **Task Name** (e.g., `Silver_to_Feature_Store`).
# MAGIC    - Set the **Type** to `Notebook`.
# MAGIC    - For **Source**, select `Workspace`.
# MAGIC    - In **Path**, navigate to the second notebook.
# MAGIC
# MAGIC 1. **Configure Compute**:
# MAGIC    - In the **Compute** section, select the cluster you have been working on.
# MAGIC
# MAGIC 1. **Configure Dependencies**:
# MAGIC    - In the **Depends On** section, notice that your previous task is automatically selected. Leave this as is.
# MAGIC
# MAGIC 1. **Add Parameters**
# MAGIC
# MAGIC 1. Click **Save task**
# MAGIC
# MAGIC 1. in the **Tasks** grid, click **+ Add task**. Repeat the steps for the third task for the third notebook.
# MAGIC
# MAGIC 1. **Create and Run the Task**:
# MAGIC    - Once all details and parameters are entered, click on **Create Task**.
# MAGIC    - After creating the task, click **Run Now** to execute it.
# MAGIC
# MAGIC Wait a moment for the pipeline to run and validate that the job was successful.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 1: Set Up Workflow Notebooks  
# MAGIC
# MAGIC The first step is to setup notebooks that will be used for Workflows. All Workflows notebooks can be found in the folder labeled **2.4 Lab - Model Training Pipeline**.
# MAGIC
# MAGIC For each notebook below, navigate to **`2.4 Lab - Model Training Pipeline`**, open the specified notebook, and fill in the necessary code. **Widgets have been preconfigured**, so you only need to focus on implementing the required logic.  
# MAGIC
# MAGIC
# MAGIC 1. Configure Notebook: **"01 Silver to Feature Store"** 
# MAGIC - Enter the task name of your choice, or alternatively, use the name of the notebook. 
# MAGIC - Navigate to **`2.4 Lab - Model Training Pipeline`** and open **`01 - Silver to Feature Store`**.  
# MAGIC - Fill in the required code where necessary.  
# MAGIC - Use the following parameters when setting up the Workflow:  
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "catalog": "dbacademy",
# MAGIC   "column": "Age",
# MAGIC   "primary_key": "id",
# MAGIC   "schema": "<your_schema>",
# MAGIC   "silver_table_name": "diabetes",
# MAGIC   "target_column": "Diabetes_binary"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC 2. Configure Notebook: **"02 Data Validation Tests"**  
# MAGIC - Enter the task name of your choice, or alternatively, use the name of the notebook.
# MAGIC - Navigate to **`2.4 Lab - Model Training Pipeline`** and open **`02 - Data Validation Tests`**.  
# MAGIC - Fill in the required code where necessary.  
# MAGIC - Use the following parameters when setting up the Workflow:  
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "catalog": "dbacademy",
# MAGIC   "schema": "<your_schema>",
# MAGIC   "silver_table_name": "diabetes"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC 3. Configure Notebook: **"03 Normalization Validation"**  
# MAGIC - Enter the task name of your choice, or alternatively, use the name of the notebook.
# MAGIC - Navigate to **`2.4 Lab - Model Training Pipeline`** and open **`03 - Normalization Validation`**.  
# MAGIC - Fill in the required code where necessary.  
# MAGIC - Use the following parameters when setting up the Workflow:  
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "catalog": "dbacademy",
# MAGIC   "schema": "<your_schema>",
# MAGIC   "normalized_column": "Age",
# MAGIC   "silver_table_name": "diabetes"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC 4. Configure Notebook: **"04 Train Models on Validated Features"**  
# MAGIC - Enter the task name of your choice, or alternatively, use the name of the notebook.
# MAGIC - Navigate to **`2.4 Lab - Model Training Pipeline`** and open **`04 - Train Models on Validated Features`**.  
# MAGIC - Fill in the required code where necessary.  
# MAGIC - Use the following parameters when setting up the Workflow:  
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "catalog": "dbacademy",
# MAGIC   "delete_column": "BMI",
# MAGIC   "primary_key": "id",
# MAGIC   "schema": "<your_schema>",
# MAGIC   "silver_table_name": "diabetes",
# MAGIC   "target_column": "Diabetes_binary"
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC 5. Configure Notebook: **"05 Serve the Model"**  
# MAGIC - Enter the task name of your choice, or alternatively, use the name of the notebook.
# MAGIC - Navigate to **`2.4 Lab - Model Training Pipeline`** and open **`05 - Serve the Model`**.  
# MAGIC - Fill in the required code where necessary.  
# MAGIC - Use the following parameters when setting up the Workflow:  
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "catalog": "dbacademy",
# MAGIC   "schema": "<your_schema>"
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Individual Notebook Testing
# MAGIC
# MAGIC After you have configured each of the notebooks, you should run them individually to squash any outstanding bugs. Waiting for an entire workflow to kick off just to have the final notebook fail can be a huge waste of time.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Chain the notebooks together to complete the model serving workflow. 
# MAGIC
# MAGIC See the notes before Step 1 if you need assistance doing this.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this lab, you created a workflow that demonstrates common testing strategies with a goal of training 2 models and serving them with Mosaic AI Model Serving. You learned how integral test strategies are the role MLflow and Unity Catalog play in accomplishing tasks involving testing. Finally, you demonstrated knowledge of splitting traffic in a 40/60 -split by configuring deployments with Mosaic AI Model Serving.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
