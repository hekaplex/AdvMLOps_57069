# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

DA = DBAcademyHelper()  # Create the DA object
DA.init()                                           # Performs basic intialization including creating schemas and catalogs

# COMMAND ----------

def init_DA(name, reset=True, pipeline=False):

    DA = DBAcademyHelper()
    if reset: DA.reset_lesson()
    DA.init()

    if pipeline:
        DA.paths.stream_source = f"{DA.paths.working_dir}/stream_source"
        DA.paths.storage_location = f"{DA.paths.working_dir}/storage_location"
        DA.pipeline_name = f"{DA.unique_name(sep='-')}: ETL Pipeline - {name.upper()}"
        DA.lookup_db = f"{DA.unique_name(sep='_')}_lookup"

        if reset:  # create lookup tables
            import pandas as pd

            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {DA.lookup_db}")
            DA.clone_source_table(f"{DA.lookup_db}.date_lookup", source_name="date-lookup")
            DA.clone_source_table(f"{DA.lookup_db}.user_lookup", source_name="user-lookup")

            df = spark.createDataFrame(pd.DataFrame([
                ("device_id_not_null", "device_id IS NOT NULL", "validity", "bpm"),
                ("device_id_valid_range", "device_id > 110000", "validity", "bpm"),
                ("heartrate_not_null", "heartrate IS NOT NULL", "validity", "bpm"),
                ("user_id_not_null", "user_id IS NOT NULL", "validity", "workout"),
                ("workout_id_not_null", "workout_id IS NOT NULL", "validity", "workout"),
                ("action_not_null","action IS NOT NULL","validity","workout"),
                ("user_id_not_null","user_id IS NOT NULL","validity","user_info"),
                ("update_type_not_null","update_type IS NOT NULL","validity","user_info")
            ], columns=["name", "condition", "tag", "topic"]))
            df.write.mode("overwrite").saveAsTable(f"{DA.lookup_db}.rules")
        
    return DA

None

# COMMAND ----------

LESSON = "generate_tokens"
DA = init_DA(LESSON, reset=False)

@DBAcademyHelper.add_method
def create_credentials_file(self, db_token, db_instance):
    contents = f"""
db_token = "{db_token}"
db_instance = "{db_instance}"
    """
    self.write_to_file(contents, "credentials.py")

None

# COMMAND ----------

import os

# set CWD to folder containing notebook; this can go away for DBR14.x+ as its default behavior
os.chdir('/Workspace' + os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()) + '/')

@DBAcademyHelper.add_method
def load_credentials(self):
    import configparser
    c = configparser.ConfigParser()
    c.read(filenames=f"var/credentials.cfg")
    try:
        token = c.get('DEFAULT', 'db_token')
        host = c.get('DEFAULT', 'db_instance')
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = token
    except:
        token = ''
        host = f'https://{spark.conf.get("spark.databricks.workspaceUrl")}'

    return token, host    

@DBAcademyHelper.add_method
def get_credentials(self):
    import ipywidgets as widgets

    (current_token, current_host) = self.load_credentials()

    @widgets.interact(host=widgets.Text(description='Host:',
                                        placeholder='Paste workspace URL here',
                                        value = current_host,
                                        continuous_update=False),
                      token=widgets.Password(description='Token:',
                                             placeholder='Paste PAT here',
                                             value = current_token,
                                             continuous_update=False)
    )
    def _f(host='', token=''):
        from urllib.parse import urlparse,urlunsplit

        u = urlparse(host)
        host = urlunsplit((u.scheme, u.netloc, '', '', ''))

        if host and token:
            os.environ["DATABRICKS_HOST"] = host
            os.environ["DATABRICKS_TOKEN"] = token

            contents = f"""
[DEFAULT]
db_token = {token}
db_instance = {host}
            """
            os.makedirs('var', exist_ok=True)
            with open("var/credentials.cfg", "w") as f:
                print(f"Credentials stored ({f.write(contents)} bytes written).")

None

# COMMAND ----------

# Method to Load GitHub Credentials
@DBAcademyHelper.add_method
def load_git_credentials(self):
    import configparser
    c = configparser.ConfigParser()
    c.read(filenames=f"var/git_credentials.cfg")
    try:
        github_username = c.get('DEFAULT', 'github_username')
        repo_name = c.get('DEFAULT', 'repo_name')
        github_token = c.get('DEFAULT', 'github_token')
    except:
        github_username = ''
        repo_name = ''
        github_token = ''

    return github_username, repo_name, github_token

# Method to Create GitHub Credentials File
@DBAcademyHelper.add_method
def create_git_credentials_file(self, github_username, repo_name, github_token):
    contents = f"""
github_username = "{github_username}"
repo_name = "{repo_name}"
github_token = "{github_token}"
    """
    self.write_to_file(contents, "git_credentials.py")
None

# Method to Get GitHub Credentials with Widgets
@DBAcademyHelper.add_method
def get_git_credentials(self):
    import ipywidgets as widgets

    (current_username, current_repo_name, current_token) = self.load_git_credentials()

    @widgets.interact(
        username=widgets.Text(
            description='Username:',
            placeholder='Enter GitHub username',
            value=current_username,
            continuous_update=False
        ),
        repo=widgets.Text(
            description='Repo Name:',
            placeholder='Enter repository name',
            value=current_repo_name,
            continuous_update=False
        ),
        token=widgets.Password(
            description='Token:',
            placeholder='Paste GitHub PAT here',
            value=current_token,
            continuous_update=False
        )
    )
    def _f(username='', repo='', token=''):
        if username and repo and token:
            contents = f"""
[DEFAULT]
github_username = {username}
repo_name = {repo}
github_token = {token}
            """
            # Save credentials in the new location
            os.makedirs('./var', exist_ok=True)
            with open("./var/git_credentials.cfg", "w") as f:
                print(f"GitHub credentials stored ({f.write(contents)} bytes written).")
None

# COMMAND ----------

# Initialize the next lesson and load credentials
LESSON = "using_cli"
DA = init_DA(LESSON, reset=False, pipeline=True)

# Load Credentials
(db_token, db_instance) = DA.load_credentials()
# Load and configure GitHub credentials
(github_token, github_username, repo_name) = DA.load_git_credentials()
