# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

import numpy as np
np.set_printoptions(precision=2)

import logging
logging.getLogger("tensorflow").setLevel(logging.ERROR)

# COMMAND ----------

@DBAcademyHelper.add_init
def create_diabetes_table_from_marketplace(self):

    import pandas as pd
    import numpy as np
    import json
    from pyspark.sql import DataFrame, functions as F, types as T
    from pyspark.sql.functions import col, monotonically_increasing_id, lit

    spark.sql(f"USE CATALOG {DA.catalog_name}")
    spark.sql(f"USE SCHEMA {DA.schema_name}")
    spark.sql(f"DROP TABLE IF EXISTS diabetes")


    shared_volume_name = 'cdc-diabetes'
    csv_name = 'diabetes_binary_5050split_BRFSS2015'

    # # Load in the dataset we wish to work on
    data_path = f"{DA.paths.datasets.cdc_diabetes}/{shared_volume_name}/{csv_name}.csv"

    diabetes_df = spark.read.csv(data_path, header="true", inferSchema="true", multiLine="true", escape='"')

    # Convert all columns to double type
    for column in diabetes_df.columns:
        diabetes_df = diabetes_df.withColumn(column, col(column).cast("double"))

    # Add a unique ID column
    diabetes_df = diabetes_df.withColumn("id", monotonically_increasing_id())

    diabetes_df.write.format('delta').saveAsTable('diabetes')

# COMMAND ----------

@DBAcademyHelper.add_init
def create_diabetes_features_table(self):
    from pyspark.sql.functions import col, monotonically_increasing_id, lit
    from pyspark.sql.types import DoubleType, StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType, MapType

    # Set the catalog and schema
    spark.sql(f"USE CATALOG {DA.catalog_name}")
    spark.sql(f"USE SCHEMA {DA.schema_name}")
    spark.sql(f"DROP TABLE IF EXISTS baseline_features")

    baseline_features_df = spark.read.format('delta').table('diabetes')

    baseline_features_df = baseline_features_df.withColumn('model_id', lit(0)).withColumn('labeled_data', col('Diabetes_binary').cast(DoubleType()))
    # Write the data back to Delta table
    baseline_features_df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", True) \
        .option("delta.enableChangeDataFeed", "true") \
        .saveAsTable(f"baseline_features__no_drift")

# COMMAND ----------

@DBAcademyHelper.add_init
def create_artificial_drift(self):
    from pyspark.sql.functions import lit, col

    baseline_pdf = spark.read.format('delta').table('baseline_features__no_drift').toPandas()

    # 1. Data Drift
    # BMI: Add a positive shift to simulate an increase in average BMI
    baseline_pdf['BMI'] = baseline_pdf['BMI'] + 15

    # Age: Add random noise to simulate measurement inconsistencies
    baseline_pdf['Age'] = baseline_pdf['Age'] + np.random.normal(0, 2, size=len(baseline_pdf))

    # PhysActivity: Set a portion of the values to 0 to simulate reduced physical activity
    phys_activity_indices = baseline_pdf.sample(frac=0.2).index  # Select 20% of the data
    baseline_pdf.loc[phys_activity_indices, 'PhysActivity'] = 0

    # 2. Concept Drift
    # HighBP: Change labels for individuals with HighBP == 1
    high_bp_indices = baseline_pdf[baseline_pdf['HighBP'] == 1].index
    baseline_pdf.loc[high_bp_indices, 'labeled_data'] = np.random.choice(
        [0, 1], p=[0.4, 0.6], size=len(high_bp_indices)
    )

    # MentHlth: Weaken the effect of poor mental health days on the target variable
    ment_hlth_indices = baseline_pdf[baseline_pdf['MentHlth'] > 10].index
    baseline_pdf.loc[ment_hlth_indices, 'labeled_data'] = np.random.choice(
        [0, 1], p=[0.7, 0.3], size=len(ment_hlth_indices)
    )

    # 3. Model Quality Drift
    # HeartDiseaseorAttack: Add random noise to reduce signal
    baseline_pdf['HeartDiseaseorAttack'] = np.random.choice(
        [0, 1], p=[0.7, 0.3], size=len(baseline_pdf)
    )

    # Education: Shuffle values to remove predictive power
    baseline_pdf['Education'] = np.random.permutation(baseline_pdf['Education'])

    # 4. Bias Drift
    # Sex: Skew the proportion of Sex values
    baseline_pdf['Sex'] = np.random.choice([0, 1], p=[0.8, 0.2], size=len(baseline_pdf))

    # Income: Remove higher-income groups to underrepresent them
    baseline_pdf.loc[baseline_pdf['Income'] > 4, 'Income'] = 4

    # Simulating baseline predictions and inference predictions
    baseline_pdf['Diabetes_binary'] = np.random.choice([0, 1], p=[0.5, 0.5], size=len(baseline_pdf))

    # Method 1: Slightly change feature distributions in the inference data
    baseline_pdf['BMI'] = baseline_pdf['BMI'] + np.random.normal(0, 1, size=len(baseline_pdf))

    baseline_df = spark.createDataFrame(baseline_pdf)

    cols_to_double = ['Diabetes_binary', 'Sex', 'HeartDiseaseorAttack']
    # Convert all columns to double type
    for column in cols_to_double:
        baseline_df = baseline_df.withColumn(column, col(column).cast("double"))

    baseline_df.write.format('delta').mode('overwrite').saveAsTable('baseline_features')

# COMMAND ----------


@DBAcademyHelper.add_init
def create_inference_table_from_marketplace(self):

  from pyspark.sql.functions import col, lit, to_date, from_json
  from pyspark.sql.types import TimestampType, StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType, MapType
  import pandas as pd

  spark.sql(f"USE CATALOG {DA.catalog_name}")
  spark.sql(f"USE SCHEMA {DA.schema_name}")

  shared_volume_name = 'monitoring'
  csv_name = 'inference_table'

  # # Load in the dataset we wish to work on
  data_path = f"{DA.paths.datasets.diabetes_inference}/{shared_volume_name}/{csv_name}.csv"

#   data_path = '/Volumes/databricks_inference_dataset_for_machine_learning_operations_associate_course/v01/monitoring/inference_table.csv'
  # Load the CSV file directly into a Spark DataFrame
  inferences_pdf = pd.read_csv(
      data_path,
      header=0,
      sep=","
  )

  inferences_df = spark.createDataFrame(inferences_pdf)

  # Define the schema for the request_metadata and response columns as a MapType or Array
  metadata_schema = MapType(StringType(), StringType())
  response_schema = StructType([
      StructField("predictions", ArrayType(DoubleType()))
  ])

  # Correct type casting for columns
  inferences_df = inferences_df.withColumn("client_request_id", col("client_request_id").cast(StringType()))
  inferences_df = inferences_df.withColumn("databricks_request_id", col("databricks_request_id").cast(StringType()))
  inferences_df = inferences_df.withColumn("timestamp_ms", col("timestamp_ms").cast(LongType()))
  inferences_df = inferences_df.withColumn("status_code", col("status_code").cast(IntegerType()))
  inferences_df = inferences_df.withColumn("execution_time_ms", col("execution_time_ms").cast(LongType()))
  inferences_df = inferences_df.withColumn("sampling_fraction", col("sampling_fraction").cast(DoubleType()))

  # Parse the date column into DateType
  inferences_df = inferences_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

  # Ensure `request_metadata`, `request`, and `response` are treated as strings before parsing
  inferences_df = inferences_df.withColumn("request_metadata", col("request_metadata").cast(StringType()))
  inferences_df = inferences_df.withColumn("request", col("request").cast(StringType()))
  inferences_df = inferences_df.withColumn("response", col("response").cast(StringType()))

  # Parse the JSON string in `request_metadata` into a MapType
  inferences_df = inferences_df.withColumn("request_metadata", from_json("request_metadata", metadata_schema))
  inferences_df = inferences_df.withColumn("response", from_json("response", response_schema))

  # Drop the existing Delta table if it exists to prevent schema conflicts
  spark.sql(f"DROP TABLE IF EXISTS model_inference_table")

  # Save the DataFrame as a Delta table in the specified schema and catalog
  inferences_df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable("model_inference_table")

  print("Inference table created successfully.")

# COMMAND ----------

@DBAcademyHelper.add_init
def create_model_logs(self):

  from pyspark.sql import functions as F
  import json
  import pandas as pd
  from pyspark.sql.dataframe import DataFrame
  import pyspark.sql.types as T
  from pyspark.sql.types import TimestampType, StructType, StructField, StringType, IntegerType, LongType, DoubleType, ArrayType, MapType
  # Define a UDF to handle JSON unpacking
  def convert_to_record_json(json_str: str) -> str:
      """
      Converts records from different accepted JSON formats into a common, record-oriented
      DataFrame format which can be parsed by the PySpark function `from_json`.
      
      :param json_str: The JSON string containing the request or response payload.
      :return: A JSON string containing the converted payload in a record-oriented format.
      """
      try:
          # Attempt to parse the JSON string
          request = json.loads(json_str)
      except json.JSONDecodeError:
          # If parsing fails, return the original string
          return json_str

      output = []
      if isinstance(request, dict):
          # Handle different JSON formats and convert to a common format
          if "dataframe_records" in request:
              output.extend(request["dataframe_records"])
          elif "dataframe_split" in request:
              dataframe_split = request["dataframe_split"]
              output.extend([dict(zip(dataframe_split["columns"], values)) for values in dataframe_split["data"]])
          elif "instances" in request:
              output.extend(request["instances"])
          elif "inputs" in request:
              output.extend([dict(zip(request["inputs"], values)) for values in zip(*request["inputs"].values())])
          elif "predictions" in request:
              output.extend([{'predictions': prediction} for prediction in request["predictions"]])
          return json.dumps(output)
      else:
          # If the format is unsupported, return the original string
          return json_str

  @F.pandas_udf(T.StringType())
  def json_consolidation_udf(json_strs: pd.Series) -> pd.Series:
      """
      A UDF to apply the JSON conversion function to every request/response.
      
      :param json_strs: A Pandas Series containing JSON strings.
      :return: A Pandas Series with the converted JSON strings.
      """
      return json_strs.apply(convert_to_record_json)

  def process_requests(requests_raw: DataFrame) -> DataFrame:
      """
      Processes a stream of raw requests and:
          - Unpacks JSON payloads for requests
          - Extracts relevant features as scalar values (first element of each array)
          - Converts Unix epoch millisecond timestamps to Spark TimestampType
      """
      # Calculate the current timestamp in seconds
      current_ts = int(spark.sql("SELECT unix_timestamp(current_timestamp())").collect()[0][0])

      # Define the start timestamp for 30 days ago
      start_ts = current_ts - 30 * 24 * 60 * 60  # 30 days in seconds

      # Dynamically calculate the min and max values of timestamp_ms
      min_max = requests_raw.agg(
          F.min("timestamp_ms").alias("min_ts"),
          F.max("timestamp_ms").alias("max_ts")
      ).collect()[0]

      min_ts = min_max["min_ts"] / 1000  # Convert from milliseconds to seconds
      max_ts = min_max["max_ts"] / 1000  # Convert from milliseconds to seconds

      # Transform timestamp_ms to span the last month
      requests_timestamped = requests_raw.withColumn(
          'timestamp', 
          (start_ts + ((F.col("timestamp_ms") / 1000 - min_ts) / (max_ts - min_ts)) * (current_ts - start_ts)).cast(TimestampType())
      ).drop("timestamp_ms")

      # Unpack JSON for the 'request' column only, since 'response' is already structured
      requests_unpacked = requests_timestamped \
          .withColumn("request", json_consolidation_udf(F.col("request"))) \
          .withColumn('request', F.from_json(F.col("request"), F.schema_of_json(
              '[{"HighBP": 1.0, "HighChol": 0.0, "CholCheck": 1.0, "BMI": 26.0, "Smoker": 0.0, "Stroke": 0.0, "HeartDiseaseorAttack": 0.0, "PhysActivity": 1.0, "Fruits": 0.0, "Veggies": 1.0, "HvyAlcoholConsump": 0.0, "AnyHealthcare": 1.0, "NoDocbcCost": 0.0, "GenHlth": 3.0, "MentHlth": 5.0, "PhysHlth": 30.0, "DiffWalk": 0.0, "Sex": 1.0, "Age": 4.0, "Education": 6.0, "Income": 8.0,"id": 1}]'))) 

      # Extract feature columns as scalar values (first element of each array)
      feature_columns = ["HighBP", "HighChol", "CholCheck", "BMI", "Smoker", "Stroke", "HeartDiseaseorAttack",
                        "PhysActivity", "Fruits", "Veggies", "HvyAlcoholConsump", "AnyHealthcare", "NoDocbcCost",
                        "GenHlth", "MentHlth", "PhysHlth", "DiffWalk", "Sex", "Age", "Education", "Income", "id"]

      for col_name in feature_columns:
          # Extract the first element of each array for all feature columns
          requests_unpacked = requests_unpacked.withColumn(col_name, F.col(f"request.{col_name}")[0])

      # Extract predictions from the 'response' column without using from_json
      requests_unpacked = requests_unpacked.withColumn("Diabetes_binary", F.col("response.predictions")[0])

      # Drop unnecessary columns
      requests_cleaned = requests_unpacked.drop("request", "response")

      # Add a placeholder 'model_id' column
      final_df = requests_cleaned.withColumn("model_id", F.lit(0).cast(T.IntegerType()))

      return final_df
  inference_df = spark.read.format('delta').table('model_inference_table')

  # Apply the function to the inference DataFrame
  model_logs_df = process_requests(inference_df.where("status_code = 200"))

  diabetes_df_pd = spark.read.format('delta').table('diabetes').toPandas()

  # Rename the column in the pandas DataFrame and convert it to a Spark DataFrame
  label_pd_df = diabetes_df_pd.rename(columns={'Diabetes_binary': 'labeled_data'})
  label_pd_df = spark.createDataFrame(label_pd_df)

  # Perform the join operation
  model_logs_df_labeled = model_logs_df.join(
      label_pd_df.select("id", "labeled_data"), 
      on=["id"], 
      how="left"
  ).drop('id')

  spark.sql("DROP TABLE IF EXISTS model_logs")

  model_logs_df_labeled.write.mode("overwrite").saveAsTable("model_logs")

  #enable change data feed for efficient execution and incremental updates to our model_logs table
  spark.sql(f'ALTER TABLE model_logs SET TBLPROPERTIES (delta.enableChangeDataFeed = true)')


  print("Model logs table created successfully.")

# COMMAND ----------

# Initialize DBAcademyHelper
DA = DBAcademyHelper() 
DA.init()
