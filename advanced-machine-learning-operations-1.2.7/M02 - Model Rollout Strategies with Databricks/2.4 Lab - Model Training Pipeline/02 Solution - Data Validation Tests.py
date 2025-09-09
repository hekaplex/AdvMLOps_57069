# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow Notebook - Data Validation Tests
# MAGIC
# MAGIC This notebook's purpose is to validate schema, missing values, and confirm nonnegative values using unittest.

# COMMAND ----------

catalog = dbutils.widgets.get('catalog')
schema = dbutils.widgets.get('schema')
silver_table_name = dbutils.widgets.get('silver_table_name')

# COMMAND ----------

spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

# DBTITLE 1,Validating Schema and Missing Values

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
        cls.df = spark.read.format("delta").table(silver_table_name).select(
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
result = unittest.TextTestRunner().run(suite)

# Check the number of failures
if len(result.failures) > 0:
    raise Exception(f"Test failed: More than 1 failure detected ({len(result.failures)} failures).")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
