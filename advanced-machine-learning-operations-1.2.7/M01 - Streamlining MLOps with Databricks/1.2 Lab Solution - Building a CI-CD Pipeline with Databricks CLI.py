# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab: Building a CI-CD Pipeline with Databricks CLI
# MAGIC
# MAGIC In this lab, you will create and execute a CI/CD pipeline for Databricks notebooks. This pipeline automates notebook execution, validation, version control, and email notifications. You'll also handle error scenarios and re-run the pipeline after resolving issues.
# MAGIC
# MAGIC
# MAGIC **Lab Outline:**
# MAGIC
# MAGIC _By the end of this lab, you will:_
# MAGIC - **Task 1 - Environment Setup: Git-Integrated Databricks Workspace**
# MAGIC   - 1.1. Configure Git Integration in Databricks  
# MAGIC   - 1.2. Clone Repository in Databricks  
# MAGIC   - 1.3. Execute Notebooks and Commit to Git Repository  
# MAGIC   - 1.4. Display Git Folder Structure  
# MAGIC   - 1.5. Pass Variables for Hyperparameter Tuning  
# MAGIC
# MAGIC - **Task 2 - Pipeline Validation Workflow with Email Notifications**
# MAGIC   - 2.1. Create Folder Structure for Workflow Configuration  
# MAGIC   - 2.2. Define Workflow Configuration  
# MAGIC   - 2.3. Save Workflow Configuration to File  
# MAGIC
# MAGIC - **Task 3 - Pipeline Execution and Version Update**
# MAGIC   - 3.1. Pipeline Execution Workflow Overview  
# MAGIC   - 3.2. Check Pipeline Status  
# MAGIC   - 3.3. Fix Errors and Re-Run the Pipeline  
# MAGIC
# MAGIC - **Task 4 - Displaying the Final Git Folder Structure**
# MAGIC
# MAGIC 📝 **Your task:** Complete the **`<FILL_IN>`** sections in the code blocks and follow the other steps as instructed.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Requirements**:
# MAGIC - Access to a **Databricks workspace** with admin rights.
# MAGIC - **GitHub repository** integrated with the workspace.
# MAGIC - Databricks CLI installed and authenticated.
# MAGIC - A basic understanding of CI/CD pipelines and Databricks workflows.

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
# MAGIC Before starting the lab, run the provided classroom setup script. This script will define configuration variables necessary for the lab. Execute the following cell:

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-01

# COMMAND ----------

# MAGIC %md
# MAGIC **Other Conventions:**
# MAGIC
# MAGIC Throughout this lab, we'll refer to the object `DA`. This object, provided by Databricks Academy, contains variables such as your username, catalog name, schema name, working directory, and dataset locations. Run the code block below to view these details:

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
# MAGIC In this Lab environment, setting up authentication for both the Databricks CLI and GitHub integration has been simplified. Follow the instructions below to ensure proper setup:
# MAGIC
# MAGIC **Databricks CLI Authentication**
# MAGIC
# MAGIC The CLI authentication process has been pre-configured for this environment. 
# MAGIC
# MAGIC Usually, you would have to set up authentication for the CLI. But in this Lab environment, that's already taken care of if you ran through the accompanying 
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
# MAGIC ## Install and Configure the Databricks CLI
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

## Retrieve the current cluster ID
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
print(f"Cluster ID: {cluster_id}")

## Get the current notebook path
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(f"Current Notebook Path: {notebook_path}")

## Define paths to related notebooks
base_path = notebook_path.rsplit('/', 1)[0] + "/1.2 Lab - Pipeline workflow notebooks"
notebook_paths = {
    "data_cleaning": f"{base_path}/01 - Data Cleaning and Transformation",
    "feature_engineering": f"{base_path}/02 Solution - Feature Engineering and Model Training",
    "failure_handling": f"{base_path}/03 - failure_handling.py",
    "model_evaluation": f"{base_path}/04 - Model Evaluation and Testing with Accuracy Check"
}
print("Notebook Paths:")
print(notebook_paths)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Task 1- Environment Setup - Git-Integrated Databricks Workspace
# MAGIC In this section, you will set up a Git-integrated Databricks workspace and clone a GitHub repository for use in this lab. Follow the steps below to ensure proper setup and integration.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Prerequisites for Git Integration with Databricks
# MAGIC Before starting, ensure you meet the following prerequisites:
# MAGIC
# MAGIC - Access to a Databricks workspace.
# MAGIC - A **GitHub** (or similar) account with a repository, such as `Adv_mlops_lab`, to integrate with Databricks.
# MAGIC - A **Personal Access Token (PAT)** from GitHub with the necessary permissions (e.g., `repo` and `workflow` scopes) to interact with your repository.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###1.1. Configure Git Integration in Databricks
# MAGIC To link your GitHub account with Databricks using a Personal Access Token:
# MAGIC 1. **Navigate to User Settings:**
# MAGIC    - In the top-right corner, click your **profile icon** and select **Settings** from the dropdown menu.
# MAGIC
# MAGIC 2. **Link GitHub with Personal Access Token:**
# MAGIC    - On the User Settings page, go to the **Linked Accounts** tab.
# MAGIC    - Under **Git Integration**, follow these steps:
# MAGIC       - Click on **Add Git credential**.
# MAGIC       - Select your Git provider (e.g., GitHub, Bitbucket Cloud) from the dropdown.
# MAGIC       - Add a **Nickname**(optional).
# MAGIC       - Choose **Personal Access Token** as the authentication method.
# MAGIC       - Enter your **GitHub username or email**.
# MAGIC       - Paste your **Personal Access Token (PAT)** into the token field.
# MAGIC       - Click **Save** to complete the integration.
# MAGIC
# MAGIC 3. **Verify Integration:**
# MAGIC    - Once complete, your Git provider will appear under the **Linked Accounts** section in the Databricks settings.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC > **Note**: A Personal Access Token provides a secure and straightforward way to connect your Git provider to Databricks.

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.2. Clone Repository in Databricks
# MAGIC The provided code automates the process of cloning your GitHub repository into the Databricks workspace. It reads Git credentials from a configuration file, sets up the local repository, and ensures that the latest changes are pulled.
# MAGIC
# MAGIC Instructions:
# MAGIC - **Prepare a GitHub Repository:**
# MAGIC   - Ensure you have a GitHub repository ready for use in this lab.
# MAGIC   - Make sure your PAT has the necessary scopes to interact with the repository.
# MAGIC
# MAGIC - **Verify the Git Credentials Configuration File:**
# MAGIC   - A configuration file named git_credentials.cfg is used to store GitHub credentials. Ensure the file includes the following fields under the [DEFAULT] section:
# MAGIC   `[DEFAULT]
# MAGIC   github_username = <your_github_username>
# MAGIC   repo_name = <your_repository_name>
# MAGIC   github_token = <your_personal_access_token>`
# MAGIC
# MAGIC - **Run the Provided Code:**
# MAGIC
# MAGIC   - The code will read your GitHub credentials, clone the repository into the Databricks workspace, and prepare it for further lab operations.
# MAGIC   - The process includes:
# MAGIC     - Reading and validating credentials from `git_credentials.cfg`.
# MAGIC     - Cloning the repository into `/Shared/<repo_name>`.
# MAGIC     - Configuring Git settings (e.g., username and email).
# MAGIC     - Pulling the latest changes from the main branch.

# COMMAND ----------

# MAGIC %md
# MAGIC **Steps:**
# MAGIC
# MAGIC **Step 1: Read GitHub Credentials**
# MAGIC - The `read_git_credentials()` function reads your **GitHub username**, **repository name**, and **PAT** from `git_credentials.cfg`. If the file or credentials are missing, it raises an error.
# MAGIC
# MAGIC **Step 2: Clone the Git Repository**
# MAGIC - The **setup_git_repo()** function:
# MAGIC
# MAGIC   - Reads credentials using **read_git_credentials()**.
# MAGIC   - Defines the local path for cloning the repository `(/Shared/<repo_name>)`.
# MAGIC   - Clones the **repository** if it doesn't exist locally.
# MAGIC   - Sets up global Git configurations (username and email).
# MAGIC   - Pulls the latest changes from the `main` branch.
# MAGIC - **Error Handling**
# MAGIC
# MAGIC   The code provides detailed error messages for missing files, incorrect credentials, or issues during Git operations.

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
    repo_name = config.get("DEFAULT", "repo_name")
    github_token = config.get("DEFAULT", "github_token")
    
    ## Validate credentials
    if not github_username or not repo_name or not github_token:
        raise ValueError("GitHub credentials are incomplete. Please provide username, repo name, and token.")
    
    ## Debugging: Ensure credentials are read correctly
    print(f"[INFO] Read Credentials -> Username: {github_username}, Repo Name: {repo_name}, Token: {github_token[:6]}... (hidden)")
    return github_username, repo_name, github_token


def setup_git_repo(config_path="var/git_credentials.cfg"):
    """
    Sets up a GitHub repository locally by cloning it and preparing it for lab operations.

    Args:
        config_path (str): Path to the configuration file containing GitHub credentials.

    Returns:
        tuple: Final username, repository name, and GitHub token used for the setup.
    """
    ## Step 1: Load credentials
    github_username, repo_name, github_token = read_git_credentials(config_path)

    try:
        ## Step 2: Define paths and repository URL
        git_repo_path = "/Shared/{repo_name}"  # Lab-specific local repo path
        repo_url = f"https://{github_username}:{github_token}@github.com/{github_username}/{repo_name}.git"

        ## Debugging: Print repository details
        print(f"[INFO] Repo Path: {git_repo_path}")
        print(f"[INFO] Repo URL: {repo_url}")

        ## Step 3: Clone or pull the latest repository updates
        if not os.path.exists(git_repo_path):
            print(f"[ACTION] Cloning the repository '{repo_name}'...")
            subprocess.run(f"git clone {repo_url} {git_repo_path}", shell=True, check=True)
        os.chdir(git_repo_path)  # Change directory to the local repo

        ## Step 4: Set up Git configuration
        print("[ACTION] Setting Git configuration...")
        subprocess.run('git config --global user.name "Databricks Lab User"', shell=True, check=True)
        subprocess.run('git config --global user.email "databricks-lab@example.com"', shell=True, check=True)

        ## Step 5: Pull the latest changes
        print("[ACTION] Pulling latest changes from the repository...")
        subprocess.run("git pull origin main --allow-unrelated-histories", shell=True, check=True)
        
        print("[SUCCESS] Git setup complete.")

    except FileNotFoundError as fnfe:
        print(f"[ERROR] {fnfe}")
    except subprocess.CalledProcessError as cpe:
        print(f"[ERROR] Git command error: {cpe}")
    except Exception as e:
        print(f"[ERROR] An error occurred while setting up Git: {e}")

    ## Step 6: Debug final values to ensure correctness
    print(f"[DEBUG] Final Values -> Username: {github_username}, Repo Name: {repo_name}, Token: {github_token[:6]}... (hidden)")

    return github_username, repo_name, github_token

## Lab-specific usage
if __name__ == "__main__":
    ## Call the function to set up the Git repository and store the final values
    final_username, final_repo_name, final_git_token = setup_git_repo()

    ## Print final values to verify correctness
    print(f"[FINAL] GitHub Username: {final_username}")
    print(f"[FINAL] Repository Name: {final_repo_name}")
    print(f"[FINAL] GitHub Token: {final_git_token[:6]}... (hidden)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3. Execute Notebooks and Commit to Git Repository
# MAGIC In this section, you will clone a GitHub repository into your Databricks workspace and set up the local repository for CI/CD operations. The provided code automates the process of cloning, verifying the repository, and ensuring the latest changes are pulled.
# MAGIC
# MAGIC **Instructions to Execute Notebooks and Commit Changes:**
# MAGIC
# MAGIC 1. **Define Repository Paths and Credentials:**
# MAGIC    - The code reads GitHub credentials (username, repository name, and personal access token) from a configuration file (`git_credentials.cfg`).
# MAGIC    - Constructs the URL and local path for the repository.
# MAGIC
# MAGIC 2. **Clone the Repository:**
# MAGIC    - The `setup_git_repo()` function clones your GitHub repository into the Databricks workspace (if not already cloned) and ensures it is up to date by pulling the latest changes.
# MAGIC
# MAGIC 3. **Pull Latest Changes:**
# MAGIC    - Ensures the latest changes from the `main` branch are pulled into the local repository.
# MAGIC
# MAGIC 4. **Set Up Git Configuration:**
# MAGIC    - Configures the Git username and email for the local repository to ensure smooth commit operations.
# MAGIC
# MAGIC 5. **Check for Changes**:
# MAGIC    - Use `git status` to check if there are any changes to be committed.
# MAGIC    - If changes are detected, add them to the staging area with `git add .`.
# MAGIC

# COMMAND ----------

import os
import shutil
import subprocess


## Define the local repository path and Git configuration
repo_url = f"https://github.com/{final_username}/{final_repo_name}.git"
local_git_repo_path = f"/Users/{DA.username}/{final_repo_name}.git"

## Function to clone the GitHub repository and set up the local environment
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
    ## Commit changes to Git if there are any changes
    os.chdir(local_git_repo_path)
    if has_changes_to_commit(local_git_repo_path):
        subprocess.run("git add .", shell=True)
        subprocess.run('git commit -m "Added exported files to Git"', shell=True, check=True)
        subprocess.run("git push origin main", shell=True, check=True)
        print("Notebooks committed and pushed to Git.")
    else:
        print("No changes to commit. Working tree is clean.")

## Function to check if there are changes to commit
def has_changes_to_commit(repo_path):
    result = subprocess.run(
        ["git", "-C", repo_path, "status", "--porcelain"],
        capture_output=True,
        text=True
    )
    return bool(result.stdout.strip())


## Setup Git repository
setup_git_repo()

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.4. Display Git Folder Structure
# MAGIC This section provides a function that prints the hierarchical folder structure of a Git repository. Understanding the structure of your repository is important for locating files, ensuring proper organization, and verifying that the repository is set up correctly.
# MAGIC
# MAGIC **Instructions:**
# MAGIC
# MAGIC - **Run the Function:**
# MAGIC
# MAGIC   - Execute the provided code after cloning your Git repository.
# MAGIC   - The code uses the path `local_git_repo_path`, which should point to the root directory of the cloned repository.
# MAGIC - **Analyze the Output:**
# MAGIC   - The printed folder structure should match the organization of your repository on GitHub.
# MAGIC   - Verify that all required files and directories (e.g., configuration files, notebooks, workflows) are present.
# MAGIC - **Debugging Tip:**
# MAGIC   - If the folder structure is incorrect or empty:
# MAGIC     - Ensure that the repository was successfully cloned.
# MAGIC     - Check the value of `local_git_repo_path` to confirm it points to the correct directory.

# COMMAND ----------

import os
def print_git_folder_structure(local_git_repo_path):
    for root, dirs, files in os.walk(local_git_repo_path):
        level = root.replace(local_git_repo_path, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print(f"{indent}{os.path.basename(root)}/")
        sub_indent = ' ' * 4 * (level + 1)
        for f in files:
            print(f"{sub_indent}{f}")

## Print the folder structure
print_git_folder_structure(local_git_repo_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ###1.5. Passing Variables for Hyperparameter Tuning
# MAGIC This section focuses on dynamically passing hyperparameters to notebooks in the CI/CD pipeline. These variables influence model training and are set as widgets, making them easy to modify without changing the code. Students will learn how to define, retrieve, and utilize these variables for experimentation and model optimization.
# MAGIC
# MAGIC **Instructions:**
# MAGIC - **Purpose of Widgets:**
# MAGIC   - Widgets allow you to pass parameters into a notebook dynamically.
# MAGIC   - By using widgets, you can easily modify hyperparameter values without altering the code.
# MAGIC - **Define Widgets:**
# MAGIC
# MAGIC     Use the `dbutils.widgets.text()` function to define the following widgets:
# MAGIC     - **max_depth:** Specifies the maximum depth of the decision tree or model.
# MAGIC     - **n_estimators:** Defines the number of estimators (e.g., trees in a forest) for the model.
# MAGIC     - **subsample:** Determines the fraction of samples to use for training.

# COMMAND ----------

## Define Widgets for Other Hyperparameters
dbutils.widgets.text("max_depth", "5", "Maximum Depth")
dbutils.widgets.text("n_estimators", "100", "Number of Estimators")
dbutils.widgets.text("subsample", "1.0", "Subsample Fraction")

## Retrieve Widget Values
max_depth = int(dbutils.widgets.get("max_depth"))
n_estimators = int(dbutils.widgets.get("n_estimators"))
subsample = float(dbutils.widgets.get("subsample"))

## Display the Retrieved Values
print(f"Using maximum depth: {max_depth}")
print(f"Using number of estimators: {n_estimators}")
print(f"Using subsample fraction: {subsample}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Task 2- Pipeline Validation Workflow with Email Notifications
# MAGIC In this section, you will set up a Databricks workflow configuration for pipeline validation. The configuration includes tasks such as data cleaning, feature engineering, conditional execution, and model evaluation. You'll also implement email notifications to monitor the success or failure of tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1. Create Folder Structure for Workflow Configuration
# MAGIC   - A dedicated folder is needed to store the workflow configuration `JSON` file.
# MAGIC   - The `os.makedirs()` function creates a folder at the specified path.
# MAGIC   - The `pipeline_config_folder` is the directory path where the JSON configuration file will be stored.
# MAGIC   - The `pipeline_config_file` is the full path, including the filename, for the workflow configuration.

# COMMAND ----------

import os
## Define folder and file paths
pipeline_config_folder = os.path.join(local_git_repo_path, "lab_pipeline_config")
pipeline_config_file = os.path.join(pipeline_config_folder, "lab_pipeline-validation-workflow.json")

## Create the folder structure
os.makedirs(pipeline_config_folder, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2. Define Workflow Configuration
# MAGIC
# MAGIC - The workflow configuration describes the sequence of tasks and their dependencies in the pipeline.
# MAGIC - Includes **email notifications** to alert you when tasks succeed or fail.
# MAGIC - **Tasks include:**
# MAGIC     - Data cleaning
# MAGIC     - Feature engineering
# MAGIC     - Conditional execution based on the success or failure of previous tasks
# MAGIC     - Model evaluation
# MAGIC - Each task is defined in the **`tasks`** array in JSON format.
# MAGIC - **`depends_on`** specifies task dependencies.
# MAGIC - **`base_parameters`** allows passing hyperparameters to notebooks.
# MAGIC - Email notifications are set for both success and failure.

# COMMAND ----------

from datetime import datetime
## Define the workflow configuration
workflow_config_pipeline = f"""
{{
  "name": "Lab Pipeline Validation Workflow with Conditional Execution - {datetime.now().strftime('%Y-%m-%d')}",
  "email_notifications": {{
    "on_failure": ["{DA.username}"],
    "on_success": ["{DA.username}"]
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
      "run_if": "ALL_SUCCESS"
    }},
    {{
      "task_key": "feature_engineering",
      "depends_on": [{{"task_key": "data_cleaning"}}],
      "notebook_task": {{
        "notebook_path": "{notebook_paths['feature_engineering']}",
        "source": "WORKSPACE",
        "base_parameters": {{
          "max_depth": "{max_depth}",
          "n_estimators": "{n_estimators}",
          "subsample": "{subsample}"
        }}
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 600,
      "run_if": "ALL_SUCCESS"
    }},
    {{
      "task_key": "conditional_execution",
      "depends_on": [{{"task_key": "feature_engineering"}}],
      "condition_task": {{
        "op": "EQUAL_TO",
        "left": "{{{{tasks.feature_engineering.values.feature_engineering_status}}}}",
        "right": "SUCCESS"
      }},
      "timeout_seconds": 0
    }},
    {{
      "task_key": "failure_handling",
      "depends_on": [{{"task_key": "conditional_execution", "outcome": "false"}}],
      "spark_python_task": {{
        "python_file": "{notebook_paths['failure_handling']}",
        "parameters": [
          "-e",
          "NonExistentColumn: Column not found in the dataset"
        ]
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 600
    }},
    {{
      "task_key": "model_evaluation",
      "depends_on": [{{"task_key": "conditional_execution", "outcome": "true"}}],
      "notebook_task": {{
        "notebook_path": "{notebook_paths['model_evaluation']}",
        "source": "WORKSPACE",
        "base_parameters": {{
          "max_depth": "{max_depth}",
          "n_estimators": "{n_estimators}",
          "subsample": "{subsample}"
        }}
      }},
      "existing_cluster_id": "{cluster_id}",
      "timeout_seconds": 600
    }}
  ]
}}
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ###2.3. Save Workflow Configuration to File
# MAGIC - Save the workflow configuration to a JSON file for later use in the pipeline execution.
# MAGIC - If the file already exists:
# MAGIC     - Prompts you for confirmation before overwriting the file.
# MAGIC     - Writes the configuration to the specified file path.

# COMMAND ----------

## Write the workflow configuration to a file
if os.path.exists(pipeline_config_file):
    user_input = input(f"The file {pipeline_config_file} already exists. Overwrite? (yes/no): ").strip().lower()
    if user_input != "yes":
        print("Operation canceled.")
    else:
        with open(pipeline_config_file, "w") as file:
            file.write(workflow_config_pipeline)
        print(f"Workflow configuration overwritten: {pipeline_config_file}")
else:
    with open(pipeline_config_file, "w") as file:
        file.write(workflow_config_pipeline)
    print(f"Workflow configuration saved: {pipeline_config_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Task 3- Pipeline Execution and Version Update
# MAGIC In this section, you will execute a Databricks pipeline using a pre-defined workflow configuration file. You will validate the pipeline's results, update its version, and commit any changes to the Git repository. This task helps automate pipeline execution and ensures proper versioning and logging of outputs.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###3.1. Pipeline Execution Workflow Overview
# MAGIC 1. **Defining Paths**
# MAGIC
# MAGIC     - **Git Repository Path:** Specifies the local location of the Git repository.
# MAGIC     - **Version File Path:** Stores pipeline version information.
# MAGIC     - **Workflow Configuration File Path:** Points to the JSON configuration file for the pipeline workflow.
# MAGIC     - **Failure Output File Path:** Captures detailed error information if tasks fail.

# COMMAND ----------

import os
import json
import subprocess

## Define paths
base_folder = f"/Workspace{base_path}"
version_file = os.path.join(local_git_repo_path, "lab_pipeline_config_version_info.json")
workflow_config_file = os.path.join(local_git_repo_path, "lab_pipeline_config", "lab_pipeline-validation-workflow.json")
failure_output_file = os.path.join(base_folder, "failure_output.json")

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **Committing and Pushing Changes to Git**
# MAGIC
# MAGIC     The `commit_and_push_changes()` function performs the following actions:
# MAGIC
# MAGIC     - Stages all changes in the repository using git add ..
# MAGIC     - Checks for uncommitted changes.
# MAGIC     - Commits changes with a message if any changes are detected.
# MAGIC     - Pushes committed changes to the `main` branch.

# COMMAND ----------

## Function to commit and push changes to Git
def commit_and_push_changes():
    try:
        os.chdir(local_git_repo_path)
        subprocess.run("git add .", shell=True, check=True)

        ## Check for changes before committing
        result = subprocess.getoutput("git status --porcelain")
        if result.strip():  # If there are changes to commit
            subprocess.run('git commit -m "Updated version and pipeline results""', shell=True, check=True)
            
            ## Push changes to the 'main' branch (or any valid branch)
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

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **Running the Pipeline**
# MAGIC
# MAGIC     1. **Execute the Pipeline**:
# MAGIC         - The function `run_pipeline_and_update_version()` will read the workflow configuration file, create a Databricks job, and execute it.
# MAGIC         - The pipeline's tasks will run sequentially as defined in the configuration.
# MAGIC
# MAGIC     2. **Monitor the Pipeline Status**:
# MAGIC         - Once the pipeline finishes executing, a **`run_page_url`** link will be generated.
# MAGIC         - This link redirects you to the Databricks job run page, where you can see the result of the pipeline execution.
# MAGIC
# MAGIC     3. **Review Task Outputs**:
# MAGIC         - The function will display a summary of all tasks in the pipeline.
# MAGIC         - For each task, it will include:
# MAGIC             - **Task Key**: The name of the task.
# MAGIC             - **Notebook Path**: The path of the notebook executed for the task.
# MAGIC             - **State**: Indicates whether the task succeeded, failed, or was skipped.
# MAGIC             - **Error Message**: If applicable, displays error details for failed tasks.
# MAGIC
# MAGIC     4. **Handle Failures**:
# MAGIC         - If any task fails, detailed error messages will be printed to help you identify the root cause.
# MAGIC         - The output will include failure details from the **failure_output.json** file if available.

# COMMAND ----------

## Function to extract and print the failure output file contents
def print_failure_output(failure_output_file):
    """
    Reads and prints the contents of the failure_output.json file if it exists.
    """
    if os.path.exists(failure_output_file):
        print("\nReading failure output file...\n")
        try:
            with open(failure_output_file, "r") as f:
                failure_output = json.load(f)
                print("\n=======\nOutput of Final Task (Failure Details):\n")
                print(json.dumps(failure_output, indent=4))
        except Exception as e:
            print(f"Error reading failure output file: {e}")
    else:
        print("No failure output file found.")
## Function to extract and print task failure details
def extract_failed_task_details(run_job_output, failure_output_file):
    """
    Parses the job output JSON to locate and print details about failed tasks, including 'failure_handling'.
    """
    try:
        run_output_json = json.loads(run_job_output)
        tasks = run_output_json.get("tasks", [])
        for task in tasks:
            task_key = task.get("task_key")
            state = task.get("state", {})
            state_message = state.get("state_message", "No state message available.")
            result_state = state.get("result_state", "Unknown")

            print(f"\nTask Key: {task_key}")
            print(f"Result State: {result_state}")
            print(f"State Message: {state_message}")

            if task_key == "failure_handling":
                if result_state == "SUCCESS":
                    print("\n=== Failure Handling Task Output ===")
                    notebook_output = task.get("notebook_output", "No output available.")
                    print(f"Notebook Output:\n{notebook_output}")
                    print_failure_output(failure_output_file)
                elif result_state == "EXCLUDED":
                    print("Task 'failure_handling' was excluded. Skipping failure output file reading.")
    except Exception as e:
        print(f"Error parsing failed task details: {e}")


## Function to extract and print final task output
def extract_and_print_final_task_output(run_page_url, tasks, failure_output_file):
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

            if task_key == "failure_handling" and notebook_path == "No Notebook Path" and state == "EXCLUDED":
                print("Feature Engineering and Model Training was successful.\n")
            elif task_key == "failure_handling" and state == "SUCCESS":
                print_failure_output(failure_output_file)
    except Exception as e:
        print(f"Error extracting task output: {e}")

## Function to run the pipeline and update the version
def run_pipeline_and_update_version():
    """
    Run the pipeline using the Databricks job API and update the version if successful.
    """
    try:
        print(f"Running pipeline using workflow config: {pipeline_config_file}")

        ## Create the Databricks job
        create_job_cmd = f"databricks jobs create --json @{pipeline_config_file}"
        job_creation_output = subprocess.getoutput(create_job_cmd)
        print(f"Job creation output: {job_creation_output}")

        ## Parse job creation output
        job_data = json.loads(job_creation_output)
        job_id = job_data.get("job_id")
        if not job_id:
            raise ValueError(f"Failed to create job. Output: {job_creation_output}")
        print(f"Job ID: {job_id}")

        ## Run the created job
        run_job_cmd = f"databricks jobs run-now {job_id}"
        run_job_output = subprocess.getoutput(run_job_cmd)
        print(f"Job run output: {run_job_output}")

        ## Parse the job run output
        job_run_data = json.loads(run_job_output)
        result_state = job_run_data.get("state", {}).get("result_state", "UNKNOWN")
        run_page_url = job_run_data.get("run_page_url", "No Run Page URL")
        tasks = job_run_data.get("tasks", [])
        print(f"Run Page URL: {run_page_url}")

        if result_state == "SUCCESS":
            print("Pipeline ran successfully.")
        else:
            print("Pipeline run failed.")
            extract_failed_task_details(run_job_output, failure_output_file)

        ## Extract final task output
        extract_and_print_final_task_output(run_page_url, tasks, failure_output_file)

        ## Update the version file
        version_data = {"version": "1.0.0"}
        if os.path.exists(version_file):
            with open(version_file, "r") as f:
                version_data = json.load(f)

        old_version = version_data["version"]
        major, minor, patch = map(int, old_version.split("."))
        version_data["version"] = f"{major}.{minor + 1}.0"

        with open(version_file, "w") as f:
            json.dump(version_data, f, indent=4)
        print(f"Version updated: {old_version} -> {version_data['version']}")
    
    except json.JSONDecodeError:
        print("Failed to parse job creation or run output. The response is not a valid JSON.")
    except Exception as e:
        print(f"Error during pipeline execution or version update: {e}")

    try:
        commit_and_push_changes()
    except Exception as e:
        print(f"Error during commit and push changes: {e}")


## Run the pipeline
run_pipeline_and_update_version()

# COMMAND ----------

# MAGIC %md
# MAGIC ###3.2. Check Pipeline Status
# MAGIC - Click on the generated `run_page_url` link after executing the pipeline function.
# MAGIC - Review the status of each task in the pipeline:
# MAGIC - If all tasks are `successful`, the pipeline has executed successfully.
# MAGIC - If any task fails, follow the `troubleshooting` steps in the task_failed.py output.
# MAGIC - **Check for Failures:**
# MAGIC   - If a task fails during execution:
# MAGIC       - Locate the failed task in the output or on the Databricks job run page.
# MAGIC       - Investigate the error by reviewing the logs available in the Databricks job run page for the failed task.
# MAGIC       - Use the provided error message or logs to identify the root cause of the failure.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3. Fix Errors and Re-Run the Pipeline
# MAGIC
# MAGIC If the pipeline fails during execution, follow these steps to troubleshoot, apply fixes, and re-run the pipeline.
# MAGIC
# MAGIC **Steps to Fix and Re-Run the Pipeline:**
# MAGIC
# MAGIC 1. **Identify the Issue**:
# MAGIC     - Review the **Output of Final Task (Failure Details)** section printed in the output.
# MAGIC     - Look for error messages or troubleshooting options associated with the failed task.
# MAGIC     - Navigate to **Jobs & Pipelines** in the **left-side menu bar** and look for your recently created job to review the job run details.
# MAGIC     - Use the job logs on the Databricks job run page to identify the cause of the failure.
# MAGIC
# MAGIC 2. **Apply Fixes**:
# MAGIC     - Open the notebook associated with the failed task.
# MAGIC     - Use the error details and logs to debug the issue.
# MAGIC     - Correct the error in the notebook or the workflow configuration file.
# MAGIC
# MAGIC 3. **Re-Run the Task**:
# MAGIC     - After applying fixes, use the provided function to re-run the pipeline:
# MAGIC       
# MAGIC       `rerun_pipeline()`
# MAGIC    
# MAGIC     - This function will re-trigger the pipeline execution, ensuring the latest changes are included.
# MAGIC
# MAGIC 4. **Commit Changes to Git Repository**:
# MAGIC     - Once the pipeline executes successfully:
# MAGIC         - Commit the updated notebooks or workflow configuration files to the Git repository for version control.
# MAGIC         - Use the `commit_and_push_changes()` function to push the changes to the repository.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Tips for Troubleshooting**
# MAGIC
# MAGIC - **Check the Failure Details**: 
# MAGIC   - Look for the **failure_output.json** file referenced in the logs for additional details.
# MAGIC   - The output of the `failure_handling` task will provide insights into specific errors.
# MAGIC
# MAGIC - **Validate Changes**:
# MAGIC   - Ensure all dependent tasks are updated and consistent with the applied fixes.
# MAGIC   - Verify notebook paths and parameters in the workflow configuration.
# MAGIC
# MAGIC - **Monitor Progress**:
# MAGIC   - Use the `run_page_url` generated after completion of the pipeline to see the status of each task.

# COMMAND ----------

## Function to commit changes to Git
def commit_and_push_changes():
    try:
        os.chdir(local_git_repo_path)
        subprocess.run("git add .", shell=True, check=True)

        ## Check for changes before committing
        result = subprocess.getoutput("git status --porcelain")
        if result.strip():  # If there are changes to commit
            subprocess.run('git commit -m "Fixed errors and updated notebooks"', shell=True, check=True)
            
            ## Push changes to the 'main' branch (or any valid branch)
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
## Function to re-run the pipeline
def rerun_pipeline():
    print("Re-running the pipeline...")
    try:
        ## Run the pipeline and update the version
        run_pipeline_and_update_version()

        ## Commit and push changes
        commit_and_push_changes()
    except Exception as e:
        print(f"Error during pipeline re-run: {e}")

## Re-run the pipeline
rerun_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4- Displaying the final Git Folder Structure
# MAGIC
# MAGIC This task helps you visualize the structure of your Git repository after executing the pipeline. By inspecting the folder hierarchy, you can confirm the organization of files and directories, ensuring that all outputs are saved correctly.
# MAGIC
# MAGIC **Instructions:**
# MAGIC
# MAGIC 1. **Repository Path Setup**:
# MAGIC    - The function `print_git_folder_structure()` accepts `local_git_repo_path` as an input, which points to the root directory of your local Git repository.
# MAGIC    - Ensure that the repository has been cloned to your Databricks environment.
# MAGIC
# MAGIC 2. **Traverse the Repository**:
# MAGIC    - The function uses the `os.walk()` method to traverse through all directories and subdirectories.
# MAGIC    - Files and folders are identified at each level.
# MAGIC
# MAGIC 3. **Print the Hierarchical Structure**:
# MAGIC    - Each directory name is displayed with an appropriate indentation to represent its level in the hierarchy.
# MAGIC    - Files within each directory are listed with further indentation.
# MAGIC
# MAGIC 4. **Run the Function**:
# MAGIC    - Execute the provided Python function to print the folder structure of the Git repository in a clear, hierarchical format.

# COMMAND ----------

## Display Git folder structure
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
# MAGIC ## Conclusion
# MAGIC
# MAGIC This lab provided hands-on experience in setting up and executing a CI/CD pipeline for Databricks notebooks. You gained practical knowledge of integrating automated validation and version control, highlighting the seamless connection between Git and Databricks workflows. By completing this lab, you have learned how to automate and ensure the robust execution of your pipeline processes effectively.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
