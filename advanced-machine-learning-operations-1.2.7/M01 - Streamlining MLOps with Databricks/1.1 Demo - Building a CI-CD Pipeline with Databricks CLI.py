# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo: Building a CI/CD Pipeline with Databricks CLI
# MAGIC
# MAGIC This demo provides a comprehensive walkthrough for building and automating a CI/CD pipeline for a Databricks project. It demonstrates how to execute tasks programmatically, validate outputs, manage version control, and set up email notifications for monitoring pipeline outcomes.
# MAGIC
# MAGIC **Learning Objectives**
# MAGIC
# MAGIC By the end of this demo, you will:
# MAGIC - Automate the execution of Databricks notebooks and validate their outputs programmatically.
# MAGIC - Integrate Git for version control, ensuring all pipeline updates are tracked and reproducible.
# MAGIC - Validate pipelines with conditional workflows and accuracy checks.
# MAGIC - Dynamically update and manage versioning information for the pipeline.
# MAGIC - Configure automated email notifications for success, failure, and conditional task execution states.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Requirements**
# MAGIC
# MAGIC To follow along with this demo, ensure the following prerequisites are met:
# MAGIC
# MAGIC 1. **Databricks Workspace**:
# MAGIC    - Access to a **Databricks workspace** with administrator permissions.
# MAGIC    - Databricks CLI installed, configured, and authenticated using a personal access token.
# MAGIC
# MAGIC 2. **Git Integration**:
# MAGIC    - A **GitHub repository** integrated with your Databricks workspace.
# MAGIC    - Personal access token (PAT) for GitHub authentication.
# MAGIC
# MAGIC 3. **Databricks Runtime**:
# MAGIC    - Use a compatible Databricks runtime version: **16.3.x-cpu-ml-scala2.12**.
# MAGIC
# MAGIC 4. **Environment Setup**:
# MAGIC    - Basic knowledge of CI/CD workflows and pipeline configurations.
# MAGIC    - Required configurations for sending email notifications (e.g., valid email addresses and permissions in Databricks).

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Classroom Setup
# MAGIC
# MAGIC Before starting the demo, run the provided classroom setup script. This script will define configuration variables necessary for the demo. Execute the following cell:

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-01

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
# MAGIC In this training environment, setting up authentication for both the Databricks CLI and GitHub integration has been simplified. Follow the instructions below to ensure proper setup:
# MAGIC
# MAGIC **Databricks CLI Authentication**
# MAGIC
# MAGIC The CLI authentication process has been pre-configured for this environment. 
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
# MAGIC **GitHub Authentication for CI/CD Integration**
# MAGIC
# MAGIC To enable CI/CD functionality, such as interacting with GitHub repositories, you need to provide your GitHub credentials, including:
# MAGIC - **GitHub Username:** Your GitHub account username.
# MAGIC - **Repository Name:** The name of the repository you want to interact with.
# MAGIC - **GitHub Token:** Your personal access token (PAT) from GitHub.
# MAGIC
# MAGIC To set or update these credentials, execute the following command:

# COMMAND ----------

DA.get_git_credentials()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Install and Configure the Databricks CLI
# MAGIC Install the Databricks CLI
# MAGIC - Use the following command to install the Databricks CLI:

# COMMAND ----------

# MAGIC %sh rm -f $(which databricks); curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.211.0/install.sh | sh

# COMMAND ----------

# MAGIC %md
# MAGIC Verify CLI installation:

# COMMAND ----------

# MAGIC %sh databricks --version

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebook Path Setup Continued
# MAGIC This code cell performs the following setup tasks:
# MAGIC
# MAGIC - Retrieves the current Databricks cluster ID and displays it.
# MAGIC - Identifies the path of the currently running notebook.
# MAGIC - Constructs paths to related notebooks for Training and deploying the model, Performance Testing,  Model Prediction Analysis, and printing the Summary report of the Model testing. These paths are printed to confirm their accuracy.

# COMMAND ----------

# MAGIC %md
# MAGIC /Workspace/Users/labuser10141814_1757436385@vocareum.com/advanced-machine-learning-operations-1.2.7/M01 - Streamlining MLOps with Databricks/1.1 Demo - Building a CI-CD Pipeline with Databricks CLI

# COMMAND ----------

# Retrieve the current cluster ID
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
print(f"Cluster ID: {cluster_id}")

# Get the current notebook path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(f"Current Notebook Path: {notebook_path}")

# Define paths to related notebooks
base_path = notebook_path.rsplit('/', 1)[0] + "/1.1 Demo - Pipeline workflow notebooks"
notebook_paths = {
    "data_cleaning": f"{base_path}/01_data_transformation/data_cleaning",
    "data_transformation": f"{base_path}/01_data_transformation/data_transformation",
}
model_notebook_paths = {
    "feature_engineering": f"{base_path}/02_model_training/feature_engineering_incorrect",
    "train_model": f"{base_path}/02_model_training/train_model",
    "evaluate_model": f"{base_path}/03_model_evaluation/evaluate_model"
}
new_notebook_paths = {
    "task_failed": f"{base_path}/02_model_training/task_failed.py",
    "accuracy_check": f"{base_path}/03_model_evaluation/accuracy_check"
}
print("Notebook Paths:")
print(notebook_paths, model_notebook_paths, new_notebook_paths)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Environment Setup - Git-Integrated Databricks Workspace
# MAGIC To get started, you will need to set up a Git-integrated Databricks workspace, clone the `<your-repo-name>` repository, and organize the notebooks into appropriate directories. This guide will walk you through the steps to set up your environment and integrate Git with Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create a Git-Integrated Databricks Workspace
# MAGIC Before we begin, ensure you meet the following prerequisites:
# MAGIC
# MAGIC - You have access to a Databricks workspace.
# MAGIC - You have a **GitHub** (or similar) account with a repository (e.g., `Adv_mlops_demo`) to integrate.
# MAGIC - A **Personal Access Token (PAT)** for your GitHub account with the necessary scopes.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Steps to Integrate Git with Databricks:
# MAGIC 1. **Access Databricks Workspace:**
# MAGIC    - Log in to your Databricks account and navigate to your workspace.
# MAGIC
# MAGIC 2. **Configure Git Integration:**
# MAGIC    - In the top-right corner, click on your **profile icon**.
# MAGIC    - Select **Settings** from the dropdown menu.
# MAGIC    - On the User Settings page, go to the **Linked Accounts** tab.
# MAGIC    - Under **Git Integration**, follow these steps:
# MAGIC      - Click on **Add Git credential**.
# MAGIC      - Choose your **Git provider** from the dropdown (e.g., GitHub, Bitbucket Cloud).
# MAGIC      - Add a **Nickname**(optional).
# MAGIC      - Select **Personal access token** as the method to authenticate.
# MAGIC      - Enter your **Git provider username or email**.
# MAGIC      - Paste your **Personal Access Token (PAT)** into the Token field. Make sure the token has the appropriate scopes (e.g., `repo` and `workflow` for GitHub).
# MAGIC      - Click **Save** to finalize the integration.
# MAGIC
# MAGIC 3. **Verify Integration:**
# MAGIC    - Once the setup is complete, your Git provider will appear as linked under the Git Integration section.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **Note**: A personal access token provides a secure and straightforward way to link your Git provider to Databricks. This method does not require admin rights on your Databricks account.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Clone Repository
# MAGIC The following code helps you clone a repository from GitHub and set up the workspace. It reads GitHub credentials, sets up the repository locally, and ensures that the latest changes are pulled.

# COMMAND ----------

import os
import subprocess
import configparser


def read_git_credentials(config_path="var/git_credentials.cfg"):
    """
    Reads GitHub credentials from a configuration file.

    Args:
        config_path (str): Path to the configuration file.

    Returns:
        tuple: GitHub username, repository name, and GitHub token.
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Git credentials file not found: {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path)

    github_username = config.get("DEFAULT", "github_username")
    repo_url = config.get("DEFAULT", "repo_name")  
    github_token = config.get("DEFAULT", "github_token")

    # Extract only the repo name from the full URL if mistakenly stored as URL
    if repo_url.startswith("https://github.com/"):
        repo_name = repo_url.split("/")[-1].replace(".git", "")
    else:
        repo_name = repo_url  # Assume it's already a clean repo name

    # Validate credentials
    if not github_username or not repo_name or not github_token:
        raise ValueError("GitHub credentials are incomplete. Please provide username, repo name, and token.")


    print(f"Read Credentials -> Username: {github_username}, Repo Name: {repo_name}, Token: {github_token[:6]}... (hidden)")
    return github_username, repo_name, github_token


def setup_git_repo(config_path="var/git_credentials.cfg"):
    """
    Sets up a GitHub repository locally by cloning it and preparing it for further operations.

    Args:
        config_path (str): Path to the configuration file containing GitHub credentials.

    Returns:
        tuple: Final username, repository name, and GitHub token used for the setup.
    """
    # Read GitHub credentials
    github_username, repo_name, github_token = read_git_credentials(config_path)

    try:
        # Define paths and repo URL
        git_repo_path = f"/Users/{DA.username}/{repo_name}"  # Local repo path
        repo_url = f"https://{github_username}:{github_token}@github.com/{github_username}/{repo_name}.git"

        # Debugging: Print repository details
        print(f"Repo Path: {git_repo_path}, Repo URL: {repo_url}")

        # Clone or update the repository
        if not os.path.exists(git_repo_path):
            print(f"Cloning the repository '{repo_name}'...")
            subprocess.run(f"git clone {repo_url} {git_repo_path}", shell=True, check=True)
        os.chdir(git_repo_path)
        print("Setting Git configuration...")
        subprocess.run('git config --global user.name "Your Name"', shell=True, check=True)
        subprocess.run('git config --global user.email "your_email@example.com"', shell=True, check=True)
        print("Pulling latest changes from the repository...")
        subprocess.run("git pull origin main", shell=True, check=True)

        print("Git setup complete.")
        print(f"Repository Name After Git Setup: {repo_name}")

    except FileNotFoundError as fnfe:
        print(f"Error: {fnfe}")
    except subprocess.CalledProcessError as cpe:
        print(f"Git command error: {cpe}")
    except Exception as e:
        print(f"Error setting up Git: {e}")

    # Final debugging: Ensure repo_name and other variables are correct after the function execution
    print(f"Final Values -> Username: {github_username}, Repo Name: {repo_name}, Token: {github_token[:6]}...")
    return github_username, repo_name, github_token


# Call the function to set up the Git repository and store the final values
final_username, final_repo_name, final_git_token = setup_git_repo()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Execute Notebooks and Commit to Git Repository
# MAGIC Once your Databricks workspace is linked to Git, you can automate the process of executing notebooks, exporting their output, and committing the changes to your Git repository. This section walks you through the steps of executing and committing notebooks.
# MAGIC
# MAGIC **Steps to Execute Notebooks and Commit Changes:**
# MAGIC
# MAGIC 1. **Define Folder Paths**:
# MAGIC    - Specify the path to the local Git repository where executed notebooks will be stored.
# MAGIC    - Create a temporary folder for exporting executed notebooks (`/tmp/notebooks_export`) if it does not exist.
# MAGIC    - Ensure a `notebooks` folder exists in the local Git repository to store the final notebook outputs.
# MAGIC
# MAGIC 2. **Run Selected Notebooks**:
# MAGIC    - Use the `dbutils.notebook.run()` function to execute specific notebooks in the pipeline, with a configurable timeout (e.g., 5 minutes per notebook).
# MAGIC    - Track successfully executed notebooks for export and log any notebooks that fail during execution.
# MAGIC
# MAGIC 3. **Export Notebooks**:
# MAGIC    - Use the `databricks workspace export-dir` command to export the successfully executed notebooks to the temporary folder.
# MAGIC
# MAGIC 4. **Copy Notebooks to Git Repository**:
# MAGIC    - Copy all exported notebooks from the temporary folder to the `notebooks` folder in the local Git repository.
# MAGIC
# MAGIC 5. **Check for Changes**:
# MAGIC    - Use the `git status` command to check for changes in the repository.
# MAGIC
# MAGIC 6. **Commit and Push Changes**:
# MAGIC    - If changes are detected, use `git add .` to stage the changes.
# MAGIC    - Commit the changes with an appropriate message, such as `"Added executed notebooks to Git"`.
# MAGIC    - Push the changes to the designated branch (e.g., `main`) in the Git repository.
# MAGIC
# MAGIC 7. **Log and Clean Up**:
# MAGIC    - Log any notebooks that failed to execute.
# MAGIC    - Remove the temporary export folder after all operations are completed to maintain a clean workspace.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Instruction:**
# MAGIC
# MAGIC This method ensures that notebooks are executed and committed to the repository with minimal manual intervention. The process provides an automated way to manage notebook execution and maintain an up-to-date repository.
# MAGIC
# MAGIC - **Git Credentials:** Ensure that the Git credentials (`github_username`, `repo_name`, `github_token`) are securely stored in a configuration file (`git_credentials.cfg`).
# MAGIC - **Timeout Configuration:** Adjust the timeout (`300 seconds`) in the `dbutils.notebook.run()` function based on the complexity of your notebooks.
# MAGIC - **Export Path Validation:** Ensure that the `databricks workspace export-dir` command successfully exports the notebooks to the specified path.
# MAGIC
# MAGIC This setup simplifies the workflow for executing, exporting, and tracking changes in your Databricks notebooks, ensuring synchronization between your workspace and Git repository.

# COMMAND ----------

import os
import shutil
import subprocess


# Define the local repository path and Git configuration
repo_url = f"https://github.com/{final_username}/{final_repo_name}.git"
local_git_repo_path = f"/Users/{DA.username}/{final_repo_name}"
new_folder_path = os.path.join(local_git_repo_path, "notebooks")  # Target folder for notebooks
notebook_export_folder = "/tmp/notebooks_export"

# Ensure export folder exists
if not os.path.exists(notebook_export_folder):
    os.makedirs(notebook_export_folder)

# Function to clone the GitHub repository and set up the local environment
def setup_git_repo():
    try:
        if not os.path.exists(local_git_repo_path):
            print(f"Cloning the repository from {repo_url}...")
            subprocess.run(f"git clone {repo_url} {local_git_repo_path}", shell=True, check=True)
        os.chdir(local_git_repo_path)
        subprocess.run("git pull origin main", shell=True, check=True)
        print("Git setup complete.")
    except subprocess.CalledProcessError as e:
        print(f"Error setting up Git repository: {e}")

# Function to check if there are changes to commit
def has_changes_to_commit(repo_path):
    result = subprocess.run(
        ["git", "-C", repo_path, "status", "--porcelain"],
        capture_output=True,
        text=True
    )
    return bool(result.stdout.strip())

# Function to execute specific notebooks and commit their outputs
def execute_and_commit_selected_notebooks():
    failed_notebooks = []
    executed_notebooks = []
    try:
        # Execute selected notebooks
        for task, path in notebook_paths.items():
            print(f"Executing notebook: {task}")
            try:
                dbutils.notebook.run(path, 300)  # 5-minute timeout per notebook
                print(f"Notebook {task} executed successfully.")
                executed_notebooks.append(task)  # Track successfully executed notebooks
            except Exception as e:
                print(f"Error executing notebook {task}: {e}")
                failed_notebooks.append(task)
        
        # Export successfully executed notebooks to a folder
        for task in executed_notebooks:
            notebook_path = notebook_paths[task]
            export_path = os.path.join(notebook_export_folder, os.path.basename(notebook_path))
            command = f"databricks workspace export --file '{export_path}' --format JUPYTER '{notebook_path}'"
            os.system(command)

        # Copy exported notebooks to Git `notebooks` folder
        if not os.path.exists(new_folder_path):
            os.makedirs(new_folder_path)
        for exported_file in os.listdir(notebook_export_folder):
            src_file = os.path.join(notebook_export_folder, exported_file)
            dest_file = os.path.join(new_folder_path, exported_file)
            shutil.copy2(src_file, dest_file)

        # Commit changes to Git if there are any changes
        os.chdir(local_git_repo_path)
        if has_changes_to_commit(local_git_repo_path):
            subprocess.run("git add .", shell=True)
            subprocess.run('git commit -m "Added executed notebooks to Git"', shell=True, check=True)
            subprocess.run("git push origin main", shell=True, check=True)
            print("Notebooks committed and pushed to Git.")
        else:
            print("No changes to commit. Working tree is clean.")
        
        # Log failed notebooks
        if failed_notebooks:
            print("\nThe following notebooks failed to execute:")
            for notebook in failed_notebooks:
                print(f"- {notebook}")
    except Exception as e:
        print(f"Error during notebook execution or Git commit: {e}")
    finally:
        if os.path.exists(notebook_export_folder):
            shutil.rmtree(notebook_export_folder)

# Setup Git repository
setup_git_repo()

# Execute and commit selected notebooks
execute_and_commit_selected_notebooks()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Display Git Folder Structure
# MAGIC This section defines a function to print the folder structure of a Git repository. It provides a clear, hierarchical view of all directories and files present within the repository.

# COMMAND ----------

local_git_repo_path = '/Workspace/Users/labuser10141814_1757436385@vocareum.com/AdvMLOps_57069'

# COMMAND ----------

def print_git_folder_structure(local_git_repo_path):
    for root, dirs, files in os.walk(local_git_repo_path):
        level = root.replace(local_git_repo_path, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print(f"{indent}{os.path.basename(root)}/")
        sub_indent = ' ' * 4 * (level + 1)
        for f in files:
            print(f"{sub_indent}{f}")

# COMMAND ----------

# Print the folder structure
print_git_folder_structure(local_git_repo_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###5. Passing Variables for Hyperparameter Tuning
# MAGIC This section demonstrates how to dynamically pass hyperparameters, such as learning rate and maximum depth, to notebooks in the CI/CD pipeline. These variables directly impact the model training process and are defined as widgets within the notebooks.

# COMMAND ----------

dbutils.widgets.text("learning_rate", "0.01", "Learning Rate")
learning_rate = float(dbutils.widgets.get("learning_rate"))
print(f"Using learning rate: {learning_rate}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pipeline Validation Workflow with Email Notifications and Required Tasks
# MAGIC
# MAGIC This section demonstrates how to define a Databricks workflow configuration for validating a pipeline. The configuration includes tasks for cleaning, transforming, and processing data, training and evaluating a model, and performing an accuracy check. Email notifications are set up to alert the user upon task success or failure.
# MAGIC
# MAGIC **Features of the Workflow Configuration**
# MAGIC
# MAGIC 1. **Email Notifications**:
# MAGIC    - Notifications are sent to the user's email address on task success or failure, as specified in the configuration.
# MAGIC
# MAGIC 2. **Required Tasks**:
# MAGIC    - Tasks such as data cleaning, transformation, feature engineering, and model evaluation are marked as **`"required": true`**. These tasks must be executed successfully before the next steps can proceed. Any failure will halt the pipeline.
# MAGIC
# MAGIC 3. **Conditional Execution**:
# MAGIC    - Conditional tasks (e.g., `conditional_execution`) allow the pipeline to decide whether to proceed with further tasks (e.g., model training) based on the success or failure of prior tasks (e.g., feature engineering).
# MAGIC
# MAGIC 4. **Dynamic Hyperparameter Passing**:
# MAGIC    - Hyperparameters such as `learning_rate` and `max_depth` are passed dynamically to model training tasks, enabling flexibility and customization during execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steps to Set Up the Pipeline Validation Workflow
# MAGIC
# MAGIC **1. Create Folder Structure for Workflow Configuration**
# MAGIC
# MAGIC - Ensure that a folder exists to store the workflow configuration JSON file. If the folder does not exist, it will be created automatically.

# COMMAND ----------

import os
# Define folder and file paths
pipeline_config_folder = os.path.join(local_git_repo_path, "pipeline_config")
pipeline_config_file = os.path.join(pipeline_config_folder, "pipeline-validation-workflow.json")

# Create the folder structure
os.makedirs(pipeline_config_folder, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC **2. Define Workflow Configuration**
# MAGIC - The workflow configuration includes various tasks such as data cleaning, transformation, feature engineering, model training, evaluation, and accuracy checks. It also includes conditional execution for tasks based on the success or failure of previous tasks.
# MAGIC
# MAGIC The following Python script creates a detailed workflow configuration:

# COMMAND ----------

from datetime import datetime
# Define the workflow configuration
workflow_config_pipeline = f"""
{{
  "name": "Demo Pipeline Validation Workflow with Conditional Execution - {datetime.now().strftime('%Y-%m-%d')}",
  "email_notifications": {{
    "on_failure": [
      "{DA.username}"
    ],
    "on_success": [
      "{DA.username}"
    ]
  }},
  "tasks": [
    {{
      "task_key": "data_cleaning",
      "notebook_task": {{
        "notebook_path": "{notebook_paths['data_cleaning']}",
        "source": "WORKSPACE"
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 600,
      "run_if": "ALL_SUCCESS",
      "email_notifications": {{
        "on_success": [
          "{DA.username}"
        ]
      }}
    }},
    {{
      "task_key": "data_transformation",
      "depends_on": [{{"task_key": "data_cleaning"}}],
      "notebook_task": {{
        "notebook_path": "{notebook_paths['data_transformation']}",
        "source": "WORKSPACE"
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 600,
      "run_if": "ALL_SUCCESS",
      "email_notifications": {{
        "on_success": [
          "{DA.username}"
        ]
      }}
    }},
    {{
      "task_key": "feature_engineering",
      "depends_on": [{{"task_key": "data_transformation"}}],
      "notebook_task": {{
        "notebook_path": "{model_notebook_paths['feature_engineering']}",
        "source": "WORKSPACE"
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 600,
      "run_if": "ALL_SUCCESS",
      "email_notifications": {{
        "on_success": [
          "{DA.username}"
        ]
      }}
    }},
    {{
      "task_key": "conditional_execution",
      "depends_on": [{{"task_key": "feature_engineering"}}],
      "condition_task": {{
        "op": "EQUAL_TO",
        "left": "{{{{tasks.feature_engineering.values.feature_engineering_status}}}}",
        "right": "SUCCESS"
      }},
      "timeout_seconds": 0,
      "email_notifications": {{}}
    }},
    {{
      "task_key": "train_model",
      "depends_on": [{{"task_key": "conditional_execution", "outcome": "true"}}],
      "notebook_task": {{
        "notebook_path": "{model_notebook_paths['train_model']}",
        "source": "WORKSPACE"
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 600,
      "run_if": "ALL_SUCCESS",
      "base_parameters": {{
        "learning_rate": "{learning_rate}",
        "max_depth": "5"
      }},
      "email_notifications": {{
        "on_success": [
          "{DA.username}"
        ]
      }}
    }},
    {{
      "task_key": "Task_Failed",
      "depends_on": [{{"task_key": "conditional_execution", "outcome": "false"}}],
      "spark_python_task": {{
        "python_file": "{new_notebook_paths['task_failed']}",
        "parameters": [
          "-e",
          "NonExistentColumn: Column not found in the dataset"
        ]
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 0,
      "run_if": "ALL_SUCCESS",
      "email_notifications": {{}}
    }},
    {{
      "task_key": "evaluate_model",
      "depends_on": [{{"task_key": "train_model"}}],
      "notebook_task": {{
        "notebook_path": "{model_notebook_paths['evaluate_model']}",
        "source": "WORKSPACE"
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 600,
      "run_if": "ALL_SUCCESS",
      "email_notifications": {{
        "on_success": [
          "{DA.username}"
        ]
      }}
    }},
    {{
      "task_key": "accuracy_check",
      "depends_on": [{{"task_key": "evaluate_model"}}],
      "notebook_task": {{
        "notebook_path": "{new_notebook_paths['accuracy_check']}",
        "source": "WORKSPACE"
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 600,
      "run_if": "ALL_SUCCESS",
      "email_notifications": {{
        "on_success": [
          "{DA.username}"
        ]
      }}
    }}
  ]
}}
"""

# COMMAND ----------

# MAGIC %md
# MAGIC **3. Save Workflow Configuration to File**
# MAGIC - This script checks if the workflow configuration file already exists. If it does, the user is prompted whether to overwrite it or not. If the file does not exist, the configuration is written to the specified path.

# COMMAND ----------

# Check if the file exists
if os.path.exists(pipeline_config_file):
    user_input = input(f"The file {pipeline_config_file} already exists. Do you want to overwrite it? (yes/no): ").strip().lower()
    if user_input != "yes":
        print("Operation canceled. File was not overwritten.")
    else:
        with open(pipeline_config_file, "w") as file:
            file.write(workflow_config_pipeline)
        print(f"Workflow configuration file overwritten at: {pipeline_config_file}")
else:
    with open(pipeline_config_file, "w") as file:
        file.write(workflow_config_pipeline)
    print(f"Workflow configuration file saved at: {pipeline_config_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Explanation of the Workflow Configuration
# MAGIC 1. **Email Notifications**: Notifications are sent to the user's email on task success or failure.
# MAGIC 2. **Required Tasks**: Tasks such as data cleaning, transformation, feature engineering, and model evaluation are marked as required. These must be successfully completed for the pipeline to continue.
# MAGIC 3. **Conditional Execution**: The `conditional_execution` task checks the status of the feature engineering task. If it is successful, the model training task proceeds.
# MAGIC 4. **Model Training and Evaluation**: The model is trained and evaluated sequentially, and the evaluation results are used to check the model's accuracy.
# MAGIC
# MAGIC By following these steps, you can effectively validate your pipeline with email notifications, required tasks, and conditional execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Execution and Version Update
# MAGIC
# MAGIC This section outlines the process of executing a Databricks pipeline using a workflow configuration file, validating the results, updating version information, and committing changes to Git.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Steps in the Process
# MAGIC
# MAGIC 1. **Defining Paths**
# MAGIC
# MAGIC The following paths are defined in the code:
# MAGIC - **Version File Path**: Stores versioning information for the pipeline.
# MAGIC - **Workflow Configuration File**: Specifies the JSON configuration for the Databricks pipeline.

# COMMAND ----------

import os
import json
import subprocess

# Define paths
version_file = os.path.join(local_git_repo_path, "pipeline_config_version_info.json")
workflow_config_file = os.path.join(local_git_repo_path, "pipeline_config", "pipeline-validation-workflow.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **Committing and Pushing Changes to Git**
# MAGIC
# MAGIC     The `commit_and_push_changes()` function performs the following actions:
# MAGIC
# MAGIC - **Staging Changes**: Adds all changes to the Git staging area using `git add .`.
# MAGIC - **Validating Changes**: Checks for uncommitted changes to avoid empty commits.
# MAGIC - **Committing and Pushing**: Commits the changes with a message and pushes them to the remote repository's `main` branch.

# COMMAND ----------

# Function to commit and push changes to Git
def commit_and_push_changes():
    try:
        subprocess.run("git add .", shell=True, check=True)
        result = subprocess.getoutput("git status --porcelain")
        if result.strip():
            subprocess.run('git commit -m "Updated version and pipeline results"', shell=True, check=True)
            subprocess.run("git push origin main", shell=True, check=True)
            print("Changes committed and pushed to Git successfully.")
        else:
            print("No changes to commit. Working tree is clean.")
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e}")
    except Exception as e:
        print(f"Error during Git operations: {e}")

# Function to extract and print task failure details
def extract_failed_task_details(run_job_output):
    try:
        # Parse the output JSON to locate failed tasks
        run_output_json = json.loads(run_job_output)
        tasks = run_output_json.get("tasks", [])
        for task in tasks:
            task_state = task.get("state", {})
            if task_state.get("result_state") == "FAILED":
                task_name = task.get("task_key")
                error_message = task_state.get("error_message", "No error message available")
                print(f"Task '{task_name}' failed with error: {error_message}")
    except Exception as e:
        print(f"Error parsing failed task details: {e}")

# COMMAND ----------

commit_and_push_changes()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **Running the Pipeline**
# MAGIC
# MAGIC     1. **Pipeline Execution**:
# MAGIC         - The function `run_pipeline_and_update_version()` is responsible for setting up and executing the pipeline.
# MAGIC         - Key steps include:
# MAGIC             - **Reading the Workflow Configuration**: The function reads the pipeline workflow configuration file to set up the Databricks job.
# MAGIC             - **Creating and Triggering the Job**:
# MAGIC                 - Uses the `databricks jobs create` command to create a job based on the configuration.
# MAGIC                 - Executes the job using `databricks jobs run-now`.
# MAGIC
# MAGIC     2. **Monitoring the Pipeline Status**:
# MAGIC         - Generates a `run_page_url` to view the pipeline execution results.
# MAGIC         - Extracts task-level details such as:
# MAGIC             - **Task Key**: Identifies the task name.
# MAGIC             - **Notebook Path**: The location of the notebook executed by the task.
# MAGIC             - **State**: The outcome of the task (e.g., SUCCESS, FAILED, or EXCLUDED).
# MAGIC             - **Error Message**: Displays the error message if the task fails or is excluded.     
# MAGIC
# MAGIC     3. **Failure Handling**:
# MAGIC         - If a task is identified as `"failure_handling"` and its state is `"SUCCESS"`, the function:
# MAGIC             - Reads the contents of the `failure_output.json` file from the workspace.
# MAGIC             - Displays detailed troubleshooting steps if the file exists.
# MAGIC         - If the `"failure_handling"` task is `"EXCLUDED"` due to unmet dependencies, the failure output file is skipped, and a message is displayed.
# MAGIC
# MAGIC     4. **Version Management**:
# MAGIC         - The pipeline version is updated after successful execution:
# MAGIC             - Reads the current version from a `version_file`.
# MAGIC             - Increments the minor version to indicate an update.
# MAGIC             - Saves the updated version back to the file.
# MAGIC
# MAGIC     5. **Commit and Push Changes**:
# MAGIC         - Any updates to the pipeline or notebooks are committed to a linked Git repository:
# MAGIC             - Adds all changes to the staging area.
# MAGIC             - Commits the changes with an appropriate message.
# MAGIC             - Pushes the changes to the GitHub repository's `main` branch.

# COMMAND ----------

import requests

# Function to extract and print final task output
def extract_and_print_final_task_output(run_page_url, tasks):
    try:
        print("\n=== Final Task Details ===")
        print(f"Run Page URL: {run_page_url}\n")
        for task in tasks:
            task_key = task.get("task_key", "Unknown Task")
            state = task.get("state", {}).get("result_state", "Unknown State")
            notebook_path = task.get("notebook_task", {}).get("notebook_path", "No Notebook Path")
            error_message = task.get("state", {}).get("state_message", "")

            print(f"Task Key: {task_key}")
            print(f"Notebook Path: {notebook_path}")
            print(f"State: {state}")
            if error_message:
                print(f"Error Message: {error_message}")
            print("====================\n")

            # Check if task has failed and the failure conditions
            if task_key == "Task_Failed" and notebook_path == "No Notebook Path" and state == "SUCCESS":
                # Path to failure output file
                failure_output_file = f"/Workspace{base_path}/failure_output.json"
                # Read and print failure output file contents if it exists
                if os.path.exists(failure_output_file):
                    print("Reading failure output file...\n")
                    with open(failure_output_file, "r") as f:
                        failure_output = json.load(f)
                        print("\n=======\nOutput of Final Task (Failure Details):\n")
                        print(json.dumps(failure_output, indent=4))
                else:
                    print("No failure output file found.")

            # Skip reading failure output if conditions match for exclusion
            if task_key == "Task_Failed" and notebook_path == "No Notebook Path" and state == "EXCLUDED":
                if error_message == "Excluded because its conditional dependency on conditional_execution was not met.":
                    print("Skipping failure output file due to exclusion condition.\n")

    except Exception as e:
        print(f"Error extracting task output: {e}")

# Function to run the pipeline and update the version
def run_pipeline_and_update_version():
    try:
        # Create and run the Databricks job from the workflow config file
        print(f"Running pipeline using workflow config: {workflow_config_file}")
        create_job_cmd = f"databricks jobs create --json @{workflow_config_file}"
        job_creation_output = subprocess.getoutput(create_job_cmd)
        print(f"Job creation output: {job_creation_output}")

        # Extract job_id
        try:
            job_id = json.loads(job_creation_output).get("job_id")
        except json.JSONDecodeError:
            raise Exception(f"Invalid response while creating job: {job_creation_output}")
        if not job_id:
            raise Exception("Failed to extract job_id from job creation output.")
        print(f"Job ID: {job_id}")

        # Run the job
        run_job_cmd = f"databricks jobs run-now {job_id}"
        run_job_output = subprocess.getoutput(run_job_cmd)
        print(f"Job run output: {run_job_output}")

        # Parse job run output
        job_run_data = json.loads(run_job_output)
        run_page_url = job_run_data.get("run_page_url", "No Run Page URL")
        tasks = job_run_data.get("tasks", [])

        # Check for success
        result_state = job_run_data.get("state", {}).get("result_state")
        if result_state == "SUCCESS":
            print("Pipeline ran successfully.")
            extract_and_print_final_task_output(run_page_url, tasks)
        else:
            print("Pipeline run failed.")
            extract_and_print_final_task_output(run_page_url, tasks)

        # Update version information
        version_data = {"version": "1.0.0"}
        if os.path.exists(version_file):
            with open(version_file, "r") as f:
                version_data = json.load(f)
        
        old_version = version_data["version"]
        major, minor, patch = map(int, old_version.split("."))
        version_data["version"] = f"{major}.{minor + 1}.0"  # Increment minor version

        with open(version_file, "w") as f:
            json.dump(version_data, f, indent=4)
        print(f"Version updated: {old_version} -> {version_data['version']}")

        # Commit and push changes
        commit_and_push_changes()

    except Exception as e:
        print(f"Error during pipeline execution or version update: {e}")

# Function to commit and push changes
def commit_and_push_changes():
    try:
        os.chdir(local_git_repo_path)
        subprocess.run("git add .", shell=True, check=True)
        subprocess.run('git commit -m "Updated version and pipeline results"', shell=True, check=True)
        subprocess.run("git push origin main", shell=True, check=True)
        print("Changes committed and pushed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e}")

# Execute the function
run_pipeline_and_update_version()

# COMMAND ----------

# MAGIC %md
# MAGIC ###6. Troubleshooting and Failure Handling
# MAGIC - **Check Task Failures:** If a task fails, detailed error messages are extracted and displayed. This includes the task name, state, and error message.
# MAGIC - **Handle Exclusions:** Tasks that are excluded due to unmet dependencies are logged but skipped.
# MAGIC - **Inspect Logs:** Use the **`run_page_url`** to view detailed logs on the Databricks job run page for failed tasks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Fix Errors and Re-Run the Pipeline
# MAGIC
# MAGIC If the pipeline fails, follow these steps:
# MAGIC
# MAGIC 1. **Identify the Issue:**
# MAGIC     - Navigate to **Jobs & Pipelines** in the **left-side menu bar** and look for your recently created job to review the job run details.
# MAGIC     - Review the logs of the failed task for error messages.
# MAGIC
# MAGIC 2. **Apply Fixes:**
# MAGIC     - Open the notebook associated with the failed task.
# MAGIC     - Debug the issue by correcting the error in the notebook.
# MAGIC         - **Solution:** Check `feature_engineering_correct` (see step #4 for the correct code) and update the code in the `feature_engineering_incorrect` notebook.
# MAGIC
# MAGIC 3. **Re-Run the Task:**
# MAGIC     - Use the provided function to re-run the pipeline after fixing the errors.
# MAGIC
# MAGIC 4. **Commit Changes to Git Repository:**
# MAGIC     - After successful execution:
# MAGIC         - Commit the updated notebooks or configuration files to the Git repository to maintain version control.
# MAGIC         - Use the `commit_and_push_changes()` function to push the changes to the repository.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Re-Run the Pipeline**
# MAGIC
# MAGIC If you need to re-run the pipeline after fixing errors, you can use the following function:
# MAGIC - This will re-trigger the pipeline execution, update the version, and commit any changes back to Git.

# COMMAND ----------

# Function to commit changes to Git
def commit_and_push_changes():
    try:
        os.chdir(local_git_repo_path)
        subprocess.run("git add .", shell=True, check=True)

        # Check for changes before committing
        result = subprocess.getoutput("git status --porcelain")
        if result.strip():  # If there are changes to commit
            subprocess.run('git commit -m "Fixed errors and updated notebooks"', shell=True, check=True)
            
            # Push changes to the 'main' branch (or any valid branch)
            branch_name = "main"  # Change this to your target branch if it's not 'main'
            push_result = subprocess.getoutput(f"git push origin {branch_name}")
            if "error" in push_result.lower():
                raise Exception(push_result)
            
            print(f"Changes committed and pushed to Git branch: {branch_name} successfully.")
        else:
            print("No changes to commit. Working tree is clean.")
    except subprocess.CalledProcessError as e:
        print(f"Git error: {e.stderr}")
    except Exception as e:
        print(f"Error during Git operations: {e}")

# Function to re-run the pipeline
def rerun_pipeline():
    print("Re-running the pipeline...")
    try:
        # Run the pipeline and update the version
        run_pipeline_and_update_version()

        # Commit and push changes
        commit_and_push_changes()
    except Exception as e:
        print(f"Error during pipeline re-run: {e}")

# Re-run the pipeline
rerun_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying Git Folder Structure
# MAGIC
# MAGIC This section demonstrates how to display the structure of the Git repository. It verifies the organization of files and directories after executing the pipeline. The function below traverses the Git repository and prints the folder structure, showing the files and directories at each level.
# MAGIC
# MAGIC **Steps:**
# MAGIC
# MAGIC 1. **Define the Git Repository Path**: The function takes the `local_git_repo_path` as input, which specifies the local Git repository's root directory.
# MAGIC    
# MAGIC 2. **Walk Through the Repository**: It walks through the directory structure, identifying files and subdirectories at each level.
# MAGIC
# MAGIC 3. **Display Folder Structure**: For each directory, it displays the directory name with proper indentation. Files within each directory are displayed with further indentation.
# MAGIC
# MAGIC 4. **Output**: The folder structure of the repository is printed in a hierarchical format.

# COMMAND ----------

# Display Git folder structure
def print_git_folder_structure(local_git_repo_path):
    for root, dirs, files in os.walk(local_git_repo_path):
        level = root.replace(local_git_repo_path, '').count(os.sep)
        indent = ' ' * 4 * level
        print(f"{indent}{os.path.basename(root)}/")
        sub_indent = ' ' * 4 * (level + 1)
        for f in files:
            print(f"{sub_indent}{f}")

print_git_folder_structure(local_git_repo_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Schedule a Notebook Job
# MAGIC To schedule a notebook job to run periodically:
# MAGIC
# MAGIC 1. In the notebook, click **Schedule** at the top right. If no jobs exist for this notebook, the Schedule dialog appears.
# MAGIC
# MAGIC ![new_schedule](../Includes/images/new_schedule.png)
# MAGIC
# MAGIC
# MAGIC > If jobs already exist for the notebook, the Jobs List dialog appears. To display the Schedule dialog, click Add a schedule.
# MAGIC
# MAGIC ![schedule](../Includes/images/schedule.png)
# MAGIC
# MAGIC 2. In the Schedule dialog, optionally enter a name for the job. The default name is the name of the notebook.
# MAGIC
# MAGIC 3. Select Simple to run your job on a simple schedule, such as every day, or Advanced to define a custom schedule for running the job, such as a specific time every day. Use the drop-downs to specify the frequency. If you choose Advanced, you can also use cron syntax to specify the frequency.
# MAGIC
# MAGIC 4. In the Compute drop-down, select the compute resource to run the task.
# MAGIC
# MAGIC     > If the notebook is attached to a SQL warehouse, the default compute is the same SQL warehouse.
# MAGIC
# MAGIC     > If your workspace is Unity Catalog-enabled and Serverless Jobs is enabled, the job runs on serverless compute by default.
# MAGIC
# MAGIC     > Otherwise, if you have Allow Cluster Creation permissions, the job runs on a new job cluster by default. To edit the configuration of the default job cluster, click Edit at the right of the field to display the cluster configuration dialog. If you do not have Allow Cluster Creation permissions, the job runs on the cluster that the notebook is attached to by default. If the notebook is not attached to a cluster, you must select a cluster from the Cluster dropdown.
# MAGIC
# MAGIC 5. Optionally, under More options, you can specify email addresses to receive Alerts on job events. See [Add email and system notifications for job events](https://docs.databricks.com/en/jobs/notifications.html).
# MAGIC
# MAGIC 6. Optionally, under More options, enter any Parameters to pass to the job. Click Add and specify the key and value of each parameter. Parameters set the value of the notebook widget specified by the key of the parameter. Use dynamic value references to pass a limited set of dynamic values as part of a parameter value.
# MAGIC
# MAGIC 7. Click **Create**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Deleting the Local Git Repository
# MAGIC
# MAGIC This function demonstrates how to delete the cloned Git repository from the Databricks environment. It removes the repository directory and all of its contents using the `rm -rf` command.

# COMMAND ----------

# Function to delete the repository locally
def delete_local_repo():
    try:
        subprocess.run(f"rm -rf {local_git_repo_path}", shell=True, check=True)
        print(f"Local repository {final_repo_name} deleted successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error deleting the local repository: {e}")
# Optionally, delete the local repo
delete_local_repo()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Deleting the GitHub Repository
# MAGIC
# MAGIC This section demonstrates how to delete a GitHub repository using the GitHub API. A valid personal access token (PAT) is required for authentication.

# COMMAND ----------

import requests

# Function to delete the GitHub repository (requires GitHub API)
def delete_github_repo():
    try:
        # GitHub API token (make sure to replace with a real token or use Databricks secrets for security)
        github_token = f"{final_git_token}"  # Replace with a valid GitHub token
        api_url = f"https://api.github.com/repos/{final_username}/{final_repo_name}"  # Correct API URL
        
        # Make a request to delete the repository on GitHub
        response = requests.delete(
            api_url,
            headers={'Authorization': f'token {github_token}'}
        )
        if response.status_code == 204:
            print(f"GitHub repository {final_repo_name} deleted successfully.")
        else:
            print(f"Failed to delete GitHub repository. Status code: {response.status_code}")
            print("Response:", response.json())  # Print the response for more details
    except Exception as e:
        print(f"Error deleting GitHub repository: {e}")

# Optionally, delete the GitHub repo
delete_github_repo()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Conclusion
# MAGIC This demo highlighted the process of setting up and executing a CI/CD pipeline for Databricks notebooks, incorporating automated validation and version control. It demonstrated how to seamlessly integrate Git with Databricks workflows, enabling efficient automation and ensuring robust pipeline execution.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
