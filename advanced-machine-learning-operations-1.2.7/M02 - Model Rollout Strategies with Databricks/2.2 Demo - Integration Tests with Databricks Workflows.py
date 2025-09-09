# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demonstration - Integration Tests with Databricks Workflows
# MAGIC
# MAGIC This demonstration guides users through creating a Model Training Pipeline Workflow in Databricks using three notebooks: **01 Silver to Feature Store for preparing features**, **02 Features Validation**, and **03 Train Model on Validated Features**. The workflow begins by setting up a Databricks job with the first notebook, adding necessary parameters, and ensuring successful execution. Dependent tasks are then added sequentially for the remaining notebooks, with each task configured to use appropriate parameters and run on the specified cluster. This process demonstrates how to build an end-to-end pipeline in Databricks Workflows, integrating data preparation, validation, and model training in a scalable and structured manner.
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC
# MAGIC By the end of this demonstration, you will be able to: 
# MAGIC
# MAGIC - Understand how to navigate the Databricks Workflows interface to create and manage jobs.
# MAGIC - Learn how to configure a Databricks job with tasks, including setting task dependencies and parameters.
# MAGIC - Develop the ability to integrate multiple notebooks into a single workflow for building an end-to-end pipeline.
# MAGIC - Demonstrate how to use Databricks Workflows to automate data preparation, feature validation, and model training tasks.
# MAGIC - Gain hands-on experience in parameterizing tasks to customize workflow execution for different data schemas and configurations.
# MAGIC - Understand the importance of task dependencies in ensuring sequential execution within a pipeline.
# MAGIC - Learn to troubleshoot and validate task success before progressing to subsequent steps in the workflow.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 1
# MAGIC
# MAGIC **🚨WARNING: The following instructions are designed to show you what a failed run looks like. DO NOT PANIC, the next cell describes what to fix.**
# MAGIC
# MAGIC Using the folder titled **2.2.1 - Model Training Pipeline**, you will find three different notebooks:
# MAGIC 1. **01 Silver to Feature Store**
# MAGIC 2. **02 Feature Validation**
# MAGIC 3. **03 Train Model on Validated Features**
# MAGIC
# MAGIC These notebooks will be used to create a workflow in Databricks.
# MAGIC
# MAGIC ## Steps to Create the Workflow
# MAGIC
# MAGIC 1. **Navigate to Jobs & Pipelines**:
# MAGIC    - Click on **Jobs & Pipelines** in the Databricks UI.
# MAGIC
# MAGIC 2. **Create a New Job**:
# MAGIC    - Click on **Create** and select **Job** from the dropdown.
# MAGIC    - Set the Job name to ***Integration Tests with Databricks Workflows*** or something similar for easy identification.
# MAGIC
# MAGIC 3. **Set Up the First Task**:
# MAGIC    - Provide a **Task Name** (e.g., `Silver to Feature Store`).
# MAGIC    - Set the **Type** to `Notebook`.
# MAGIC    - For the **Source**, select `Workspace`.
# MAGIC    - In **Path**, navigate to the first notebook: `01 Silver to Feature Store`.
# MAGIC
# MAGIC 4. **Configure Compute**:
# MAGIC    - In the **Compute** section, select the cluster you have been working on.
# MAGIC
# MAGIC 5. **Add Parameters**:
# MAGIC    - In the **Parameters** section, add the required parameters for the notebook using the keys and values given here (key: value):
# MAGIC      - **catalog: dbacademy**
# MAGIC      - **column: Age**
# MAGIC      - **schema: <your schema>**
# MAGIC      - **silver_table_name: diabetes**
# MAGIC      - **target_column: Diabetes_binary**
# MAGIC    Alternatively, you can click on JSON next to the parameter entries and copy and paste the following:
# MAGIC
# MAGIC    ```
# MAGIC       {
# MAGIC          "catalog": "dbacademy",
# MAGIC          "target_column": "Diabetes_binary",
# MAGIC          "schema": "<your_schema>",
# MAGIC          "column": "Age",
# MAGIC          "silver_table_name": "diabetes"
# MAGIC       }
# MAGIC    ```
# MAGIC
# MAGIC 6. **Create the Task**:
# MAGIC    - Once all details and parameters are entered, click on **Create Task**.
# MAGIC 7. Click **Run now** at the top right.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handling the Error
# MAGIC
# MAGIC 1. **Navigate to Jobs & Pipelines**:
# MAGIC    - In the left sidebar, click on **Jobs & Pipelines**.
# MAGIC    - Find the job you just ran.
# MAGIC
# MAGIC 2. **Identify the Failed Run**:
# MAGIC    - Look to the right under **Recent Runs**.
# MAGIC    - Find the **red X** with a circle around it, indicating a failed run.
# MAGIC    - Click on the red X to view details about the failure.
# MAGIC
# MAGIC 3. **Analyze the Error**:
# MAGIC    - On the screen, you'll see the error message explaining why the job failed.
# MAGIC    - In this case, the error states: **"No input widget named primary_key is defined."**
# MAGIC    - This happened because we forgot to define the **primary key** parameter when setting up the task.
# MAGIC
# MAGIC 4. **Fix the Issue**:
# MAGIC    - Navigate to the top right and click **Edit Task**, located next to **Repair Run**.
# MAGIC    - Go to the **Parameters** section and click **Add**.
# MAGIC    - Enter the following:
# MAGIC      - **Key**: `primary_key`
# MAGIC      - **Value**: `id`
# MAGIC    - Click **Save Task**.
# MAGIC
# MAGIC 5. **Re-run the Workflow**:
# MAGIC    - Click **Run Now** at the top right.
# MAGIC    - Once the run starts, a message box will appear. Click **View Run** in the message box.
# MAGIC    - This will take you to the notebook, where you can watch the notebook run successfully.
# MAGIC
# MAGIC By correcting the parameter setup and re-running the task, the workflow will now execute as expected.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Step 2: Add and Run the Remaining Tasks
# MAGIC
# MAGIC **1. Navigate to Jobs & Pipelines:**
# MAGIC - Click on **Jobs & Pipelines** in the Databricks UI.
# MAGIC - Open the job you just created.
# MAGIC
# MAGIC **2. Add the Feature Validation Task:**
# MAGIC   1. Click on the **Tasks** tab.
# MAGIC   2. Click on the **Add Task** button.
# MAGIC   3. Set up the task:
# MAGIC     - **Task Name**: `Feature Validation`
# MAGIC     - **Type**: `Notebook`
# MAGIC     - **Source**: `Workspace`
# MAGIC     - **Path**: Select the notebook **`02 Feature Validation`**.
# MAGIC   4. Configure **Compute**:
# MAGIC     - Choose the **same cluster** as the previous task.
# MAGIC   5. Set **Task Dependencies**:
# MAGIC     - In the **Depends On** section, ensure it is set to **`Silver to Feature Store`**.
# MAGIC   6. **Add Parameters**:
# MAGIC     - Copy and paste the following parameters:
# MAGIC       ```json
# MAGIC       {
# MAGIC         "catalog": "dbacademy",
# MAGIC         "normalized_column": "Age",
# MAGIC         "schema": "<your_schema>"
# MAGIC       }
# MAGIC       ```
# MAGIC   7. Click **Create Task**.
# MAGIC
# MAGIC **3. Add the Train Model on Features Task:**
# MAGIC
# MAGIC Now, we need to add the **third task** to the workflow, which will train the model on validated features.
# MAGIC
# MAGIC   1. Click on **Add Task** again.
# MAGIC   2. Set up the **third task**:
# MAGIC
# MAGIC       - **Task Name**: `Train Model on Features` (or any preferred name).
# MAGIC       - **Type**: `Notebook`.
# MAGIC       - **Source**: `Workspace`.
# MAGIC       - **Path**: Navigate to the **third notebook**: `03 Train Model on Validated Features`.
# MAGIC
# MAGIC   3. Configure **Compute**:
# MAGIC     - Choose the **same cluster** as the previous tasks.
# MAGIC   4. **Set Task Dependencies**:
# MAGIC     - In the **Depends On** section, select **`Feature Validation`**.
# MAGIC   5. **Add Parameters**:
# MAGIC     - Copy and paste the following parameters:
# MAGIC       ```json
# MAGIC       {
# MAGIC         "catalog": "dbacademy",
# MAGIC         "username": "<your_username>",
# MAGIC         "target_column": "Diabetes_binary",
# MAGIC         "schema": "<your_schema>",
# MAGIC         "primary_key": "id",
# MAGIC         "silver_table_name": "diabetes"
# MAGIC       }
# MAGIC       ```
# MAGIC   6. Click **Create Task**.
# MAGIC
# MAGIC **4. Run the Workflow**
# MAGIC   1. Once all tasks are created, click **Run Now**.
# MAGIC   2. Wait for the pipeline to complete execution.
# MAGIC   3. Ensure that **all tasks run successfully** before proceeding to the next step.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Verify Model Registration in Unity Catalog
# MAGIC After completing Step 2, the final step is to confirm that the trained model has been successfully **logged in Unity Catalog**.
# MAGIC
# MAGIC **1. Check the Model Naming Convention**
# MAGIC - The model will be named following this pattern:
# MAGIC   ```
# MAGIC   dbacademy.<schema_name>.workflows_classifier_model
# MAGIC   ```
# MAGIC   - Replace `<schema_name>` with the **schema name you used in Step 2**.
# MAGIC
# MAGIC **2. Locate the Model in Unity Catalog**
# MAGIC 1. Navigate to **Catalog** in the Databricks UI.
# MAGIC 2. Open the **schema** you have been working on.
# MAGIC 3. Look for the model named **`workflows_classifier_model`**.
# MAGIC
# MAGIC **3. Verify Model Lineage (Optional)**
# MAGIC - You can inspect the model lineage within **Catalog Explorer**:
# MAGIC   1. Click on the **model version**.
# MAGIC   2. Select **Lineage**.
# MAGIC   3. Choose **Workflows** to see that this model was created using the workflow you just ran.
# MAGIC
# MAGIC By confirming the model's presence in Unity Catalog, you ensure that the **end-to-end workflow has successfully stored and registered the model for future use**.

# COMMAND ----------

# MAGIC %md
# MAGIC Additionally, you can inspect the model lineage within **Catalog Explorer** by clicking on the model version and selecting lineage. Select **Models** to see that this model was created using the job you created.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Concluding Remarks
# MAGIC
# MAGIC In this demonstration, we explored how to perform an **integration test** using **Databricks Workflows**. The process involved the following steps:
# MAGIC
# MAGIC 1. **Creating a Simple Job**:
# MAGIC    - We started by creating a workflow that read silver-layered data from **Unity Catalog**, transformed it, and created a **feature table** stored in the **Databricks Feature Store**.
# MAGIC
# MAGIC 2. **Validating Features**:
# MAGIC    - We attached a second notebook to the workflow to validate that the features in the feature table were behaving as expected, such as confirming normalization.
# MAGIC
# MAGIC 3. **Training a Model**:
# MAGIC    - A third notebook was added to train a model using the validated features, and the resulting model was stored in **Unity Catalog** for centralized management.
# MAGIC
# MAGIC By completing this workflow, we demonstrated how to build and validate an end-to-end pipeline for machine learning using Databricks Workflows, ensuring both the data and model meet expectations at every step.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
