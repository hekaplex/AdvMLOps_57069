# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Advanced Machine Learning Operations
# MAGIC
# MAGIC In this course, you will be provided with a comprehensive understanding of the machine learning lifecycle and MLOps, emphasizing best practices for data and model management, testing, and scalable architectures. It covers key MLOps components, including CI/CD, pipeline management, and environment separation, while showcasing Databricks’ tools for automation and infrastructure management, such as Databricks Asset Bundles (DABs), Workflows, and Mosaic AI Model Serving. 
# MAGIC
# MAGIC You will learn about monitoring, custom metrics, drift detection, model rollout strategies, A/B testing, and the principles of reliable MLOps systems, providing a holistic view of implementing and managing ML projects in Databricks.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Prerequisites
# MAGIC The content was developed for participants with these skills/knowledge/abilities:  
# MAGIC - The user should have intermediate-level knowledge of traditional machine learning concepts, development, and the use of Python and Git for ML projects.
# MAGIC - It is recommended that the user has intermediate-level experience with Python. 
# MAGIC - Databricks Academy content is designed to be run on the provided Vocareum environment only. 
# MAGIC - Databricks does not guarantee that the content can be run successfully on any other instances of a Databricks Workspace.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Course Agenda  
# MAGIC The following modules are part of the **Advanced Machine Learning Operations** course by **Databricks Academy**.
# MAGIC
# MAGIC | # | Module Name | Lesson Name |
# MAGIC |---|-------------|-------------|
# MAGIC | 1 | [Streamlining MLOps with Databricks]($./M01 - Streamlining MLOps with Databricks) | • *Lecture:* Streamlining MLOps <br> • *Lecture:* Streamlining MLOps with Databricks <br> • [Setup: Generate Tokens]($./M01 - Streamlining MLOps with Databricks/0 - Generate Tokens) <br> • [Demo: Building a CI-CD Pipeline with Databricks CLI]($./M01 - Streamlining MLOps with Databricks/1.1 Demo - Building a CI-CD Pipeline with Databricks CLI) <br> • [Lab: Building a CI-CD Pipeline with Databricks CLI]($./M01 - Streamlining MLOps with Databricks/1.2 Lab - Building a CI-CD Pipeline with Databricks CLI) |
# MAGIC | 2 | [Model Rollout Strategies with Databricks]($./M02 - Model Rollout Strategies with Databricks) | • *Lecture:* Automate Comprehensive Testing <br> • *Lecture:* Model Rollout Strategies with Databricks <br> • [Demo: Common Testing Strategies]($./M02 - Model Rollout Strategies with Databricks/2.1 Demo - Common Testing Strategies) <br> • [Demo: Integration Tests with Databricks Workflows]($./M02 - Model Rollout Strategies with Databricks/2.2 Demo - Integration Tests with Databricks Workflows) <br> • [Demo: Model Rollout Strategies with Mosaic AI Model Serving]($./M02 - Model Rollout Strategies with Databricks/2.3 Demo - Model Rollout Strategies with Mosaic AI Model Serving) <br> • [Lab: Rollout Strategies with Workflows]($./M02 - Model Rollout Strategies with Databricks/2.4 Lab - Rollout Strategies with Workflows) |
# MAGIC | 3 | [Lakehouse Monitoring]($./M03 - Lakehouse Monitoring) | • *Lecture:* Lakehouse Monitoring <br> • [Demo: Monitoring Model Quality]($./M03 - Lakehouse Monitoring/3.1 Demo - Monitoring Model Quality) <br> • [Lab: Monitoring Drift with Lakehouse Monitoring]($./M03 - Lakehouse Monitoring/3.2 Lab - Monitoring Drift with Lakehouse Monitoring) |
# MAGIC | 4 | [Build ML Assets as Code]($./M04 - Build ML Assets as Code) | • *Lecture:* Build ML Assets as Code <br> • [Setup: Generate Tokens]($./M04 - Build ML Assets as Code/0 - Generate Tokens) <br> • [Demo: Working with Asset Bundles]($./M04 - Build ML Assets as Code/4.1 Demo - Working with Asset Bundles) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC
# MAGIC ## Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * Use Databricks Runtime version: **`16.3.x-cpu-ml-scala2.12`** for running all demo and lab notebooks
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Troubleshooting
# MAGIC **Instruction:** In case of any issues, please refer to the [Troubleshooting Content Notebook]($./TROUBLESHOOTING_CONTENT).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
