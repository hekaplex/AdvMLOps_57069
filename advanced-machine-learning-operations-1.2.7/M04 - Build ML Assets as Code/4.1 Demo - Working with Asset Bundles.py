# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo - Working with Asset Bundles
# MAGIC
# MAGIC Databricks Asset Bundles are an excellent way to develop complex projects in your own development environment and deploy them to your Databricks workspace. You can use common CI/CD practices to keep track of the history of your workflow.
# MAGIC
# MAGIC In this demo, we will show you how to use a Databricks Asset Bundle to start, deploy, run, and destroy a simple workflow job.
# MAGIC
# MAGIC Normally, the process of working with asset bundles would be done in a terminal on your local computer or through a CI/CD workflow configured on your repository system (e.g., GitHub). In this demo, we will use shell scripts that run on the driver of the current cluster.

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
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run this notebook, you need to use one of the following Databricks runtime(s): **16.3.x-cpu-ml-scala2.12**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚨Authentication
# MAGIC
# MAGIC In this training environment, setting up authentication for the Databricks CLI has been simplified. Follow the instructions below to ensure proper setup:
# MAGIC
# MAGIC **Databricks CLI Authentication**
# MAGIC
# MAGIC The CLI authentication process has been pre-configured for this environment. 
# MAGIC
# MAGIC Usually, you would have to set up authentication for the CLI. But in this training environment, that's already taken care of if you ran through the accompanying 
# MAGIC **'Generate Tokens'** notebook. 
# MAGIC If you did, credentials will already be loaded into the **`DATABRICKS_HOST`** and **`DATABRICKS_TOKEN`** environment variables. 
# MAGIC
# MAGIC **✅ If you have already run the "Generate Tokens" notebook, you are good to go!
# MAGIC ❌ If you have NOT run it yet, please do so now, then restart this notebook.**

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script. This script will define configuration variables necessary for the demo. Execute the following cell:

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-4.1

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
# MAGIC ### Authentication
# MAGIC
# MAGIC Usually, you would have to set up authentication for the CLI. But in this training environment, that's already taken care of if you ran through the accompanying 
# MAGIC **'Generate Tokens'** notebook. 
# MAGIC If you did, credentials will already be loaded into the **`DATABRICKS_HOST`** and **`DATABRICKS_TOKEN`** environment variables. 
# MAGIC
# MAGIC #####*If you did not, run through it now then restart this notebook.*

# COMMAND ----------

DA.get_credentials()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install CLI
# MAGIC
# MAGIC Install the Databricks CLI using the following cell. Note that this procedure removes any existing version that may already be installed, and installs the newest version of the [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html). A legacy version exists that is distributed through **`pip`**, however we recommend following the procedure here to install the newer one.

# COMMAND ----------

# MAGIC %sh 
# MAGIC # Remove existing CLI if present
# MAGIC which databricks && rm $(which databricks);
# MAGIC [ -e $HOME/bin/databricks ] && rm $HOME/bin/databricks
# MAGIC # Download and install new version
# MAGIC curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.238.0/install.sh | sh;
# MAGIC # Make it easily accessible
# MAGIC ln -s $HOME/bin/databricks /usr/local/bin

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating a Databricks Asset Bundle from a Template
# MAGIC
# MAGIC There are a handful of templates available that allow you to quickly create an asset bundle. We are going to use a python-based bundle that will generate a Databricks Workflow Job.
# MAGIC
# MAGIC In your day-to-day work, you can start with a template and make changes to it as needed, or you can create [your own templates](https://docs.databricks.com/en/dev-tools/bundles/templates.html).
# MAGIC
# MAGIC The command below invokes the Databricks CLI and uses the `init` command from the `bundle` command group to create and initialize a bundle from the `default-python` template.

# COMMAND ----------

# MAGIC %sh 
# MAGIC databricks catalogs list

# COMMAND ----------

# MAGIC %sh 
# MAGIC databricks bundle init default-python

# COMMAND ----------

# MAGIC %sh
# MAGIC cd my_project
# MAGIC databricks bundle validate

# COMMAND ----------

# MAGIC %md
# MAGIC ## The `my_project` Directory
# MAGIC
# MAGIC As you can see from the output of the cell above, our project has been created in the `my_project` directory. Note that this is in the same directory as this notebook. You may need to refresh this page in your browser.
# MAGIC
# MAGIC Open the folder and note all the sub-directories and files contained within.
# MAGIC
# MAGIC We are going to make a few changes before we deploy this bundle. We are going to change the job configuration to use the same cluster we are currently using, instead of a Jobs Cluster. This is not recommended practice, but we are doing it just for the sake of this course.
# MAGIC
# MAGIC Run the cell below to make these changes.

# COMMAND ----------

DA.update_bundle()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Databricks Asset Bundle
# MAGIC
# MAGIC In the command below, we first `cd` into the `my_project` directory (since `my_project` is the root of the asset bundle). We then run `validate`. This command allows us to ensure our files are syntactically correct.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd my_project
# MAGIC databricks bundle validate

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deploying Databricks Asset Bundles
# MAGIC
# MAGIC We can deploy the bundle to our workspace by running the simple command below.
# MAGIC
# MAGIC After the deployment is complete, we will have a new workflow job in our workspace.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd my_project
# MAGIC databricks bundle deploy -t prod

# COMMAND ----------

# MAGIC %md
# MAGIC ## View the Created Job
# MAGIC
# MAGIC 1. Click **`Jobs & Pipelines`** in the left sidebar menu, and search for `[dev {DA.username}] my_project_job`.
# MAGIC 2. Click the name of the job.
# MAGIC 3. Click the **`Tasks`** tab.
# MAGIC
# MAGIC Note that there are two tasks in the job. Also, note that the job is connected to a Databricks Asset Bundle and that we should not edit the job directly in the workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Job
# MAGIC
# MAGIC We could run the job manually within the workspace, but we will run the job using `run` from the `bundle` command group:

# COMMAND ----------

# MAGIC %sh
# MAGIC cd my_project
# MAGIC databricks bundle summary -t prod

# COMMAND ----------

# MAGIC %sh
# MAGIC cd my_project
# MAGIC databricks bundle run my_project_job -t prod

# COMMAND ----------

# MAGIC %sh
# MAGIC job_id=$(databricks jobs list | awk '/my_project_job/ {print $1; exit}')
# MAGIC echo "Job ID: $job_id"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting Up Monitoring and Notifications
# MAGIC
# MAGIC
# MAGIC To add monitoring and notifications, we can use the Databricks API to configure job notifications programmatically. Below is an example of how to do this using Python:
# MAGIC
# MAGIC

# COMMAND ----------

# Use the Token class to get the token
token_obj = Token()
token = token_obj.token

# COMMAND ----------

import requests
import json
import subprocess

# Replace these with your own values
databricks_instance = f"https://{spark.conf.get('spark.databricks.workspaceUrl')}"
api_token = token.strip("{}").strip("'")
# Run the shell command to get the job ID
result = subprocess.run(["databricks", "jobs", "list"], capture_output=True, text=True)
job_id = next((line.split()[0] for line in result.stdout.splitlines() if "my_project_job" in line), None)

# Define the notification settings
notification_settings = {
    "settings": {
        "email_notifications": {
            "on_failure": [f"{DA.username}"],
            "on_start": [],
            "on_success": []
        }
    }
}

# Make the API request to update job settings
response = requests.post(
    f"{databricks_instance}/api/2.0/jobs/update",
    headers={"Authorization": f"Bearer {api_token}"},
    json={
        "job_id": job_id,
        "new_settings": notification_settings["settings"]
    }
)

# Check the response
if response.status_code == 200:
    print("Notification settings updated successfully.")
else:
    print(f"Failed to update notification settings: {response.text}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modifying and Redeploying the Bundle
# MAGIC
# MAGIC To emphasize job exploration and modification, let’s modify a part of the job and redeploy it. For instance, we can update the Python script in the bundle to include additional logging or a simple transformation.
# MAGIC
# MAGIC 1. Open the `my_project` directory and locate the Python script used in the job.  (my_project/src/my_project/main.py)
# MAGIC 2. Make necessary modifications (e.g., add logging).
# MAGIC       Example: Add the lines of code below "get_taxis().show(5)"
# MAGIC
# MAGIC
# MAGIC ```python
# MAGIC # Add the following line to your script for additional logging
# MAGIC print("Executing job with modified script")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ###Redeploy the Updated Bundle
# MAGIC Redeploy the updated bundle to apply the changes.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd my_project
# MAGIC databricks bundle deploy

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the Modified Job
# MAGIC
# MAGIC Run the modified job to ensure the changes have been applied.
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC cd my_project
# MAGIC databricks bundle run my_project_job

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Sample DAB for an ML Workflow
# MAGIC
# MAGIC A preconfigured ML workflow has been setup for you as a part of this demonstration. Within this notebook's folder, navigate to `ml_project`. There you will find a folder called `src` and another folder called `resources`.
# MAGIC
# MAGIC - `src` will contain all the notebooks needed for the sample project. 
# MAGIC - `resources` will contain all the YAML files needed. 
# MAGIC - Take a moment to investigate the YAML files in **resources** and inspect source code files located in **src**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Default Variable configurations:
# MAGIC There are a few variables to mention that are contained in the YAML files for `ml_project`.
# MAGIC ### Cluster ID: `cluster_id_var`
# MAGIC You can explicitly define your cluster ID by going to the **variables.yml** file and change the default value for the variable `cluster_id_var` to the output for the next cell. 
# MAGIC
# MAGIC ### Host
# MAGIC Notice that the `databricks.yml` file does not contain any mention of your Workspace URL. This is because we have defined the DATABRICKS_HOST variable as a part of the setup for this demonstration. You can see this value printed after the next cell. 
# MAGIC
# MAGIC ### Username: `username_var`
# MAGIC You can explicitly define your username with the `username_var` variable. However, note that the user that deploys the bundle to Workflows will have that value set by default using `${workspace.current_user.userName}`. You can use the schema hierarchy documented [here](https://docs.databricks.com/api/workspace/introduction). 
# MAGIC
# MAGIC #### Instructions:
# MAGIC Navigate to the `variables.yml` file and change the default value of the cluster configuration to your unique cluster ID. You can find this ID by running the next cell (along with the `DATABRICKS_HOST` variable value).
# MAGIC
# MAGIC *Note: This is a simple setup and is not meant to be complex for demonstration purposes.*

# COMMAND ----------

# MAGIC %md
# MAGIC Here is how you can view your cluster ID and `DATABRICKS_HOST` variables.

# COMMAND ----------

print("Cluster ID:", spark.conf.get("spark.databricks.clusterUsageTags.clusterId"))
print("DATABRICKS_HOST:", os.environ.get('DATABRICKS_HOST'))

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ml_project
# MAGIC databricks bundle validate -t development

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ml_project
# MAGIC databricks bundle deploy

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## View the Created Job and Run with the UI
# MAGIC
# MAGIC 1. Click **`Workflows`** in the left sidebar menu, and search for `[dev {DA.username}] ml_project_job`.
# MAGIC 2. Click the name of the job.
# MAGIC 3. Click the **`Tasks`** tab.
# MAGIC
# MAGIC Note that there are a series of tasks created that demonstrate a simple workflow common in machine learning. This particular setup is designed to (successfully) fail at the conditional fork within the workflow. This means the model will not be evaluated or checked for accuracy. This will be indicated by **Task_Failed** displaying a green banner.
# MAGIC
# MAGIC ## Optional
# MAGIC You can also run the job using the following code:
# MAGIC
# MAGIC ```
# MAGIC %sh
# MAGIC cd ml_project
# MAGIC databricks bundle run ml_project_job
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Destroying both Bundles
# MAGIC
# MAGIC Notice that we can complete all of these tasks without ever being in a workspace, as long as we have a token configured.
# MAGIC
# MAGIC Our last step is to destroy the deployed infrastructure. Notice that with this approach (infrastructure as code) deploying, running, and destroying the bundles is very simple using the Databricks CLI.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd my_project
# MAGIC databricks bundle destroy --auto-approve

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ml_project
# MAGIC databricks bundle destroy --auto-approve

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion
# MAGIC
# MAGIC In this demo, we focused on building, modifying, and exploring jobs using Databricks Asset Bundles within an MLOps context. We ensured the steps align with best practices for operationalizing machine learning models, including job execution, CI/CD integration, and monitoring. We also demonstrated how to modify and redeploy jobs to adapt to changing requirements.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
