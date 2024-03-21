# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <img src="https://databricks.gallerycdn.vsassets.io/extensions/databricks/databricks/1.1.5/1696858282359/Microsoft.VisualStudio.Services.Icons.Default" alt="iconDatabricks" width="100"/>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC ## Databricks with PySpark
# MAGIC ------
# MAGIC
# MAGIC
# MAGIC - What is PySpark?
# MAGIC - Notebooks
# MAGIC - Clusters
# MAGIC - dbUtils
# MAGIC - FileSystem 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is PySpark?
# MAGIC <br>
# MAGIC PySpark is the Python library for Apache Spark. <br>
# MAGIC PySpark provides a user-friendly API for interacting with Spark's distributed computing capabilities.
# MAGIC
# MAGIC <br>
# MAGIC <br>
# MAGIC
# MAGIC <img src="https://3.bp.blogspot.com/-tlCzGQ9Tslw/Wn3rA1eJM4I/AAAAAAAAEE4/nmHxKp3qWbkz1Ehzv792izraR_wxjEKhQCLcBGAs/s1600/ApacheSpark.JPG" alt="iconDatabricks" width="400"/>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC PySpark supports all of Spark’s features (Modules) such as Spark SQL, DataFrames, Structured Streaming, Machine Learning (MLlib) and Spark Core.
# MAGIC
# MAGIC [PySpark Docs](https://spark.apache.org/docs/latest/api/python/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is a Cluster?
# MAGIC An Azure Databricks cluster is a set of computation resources and configurations on which you run data engineering, data science, and data analytics workloads, such as production ETL pipelines, streaming analytics, ad-hoc analytics, and machine learning. 
# MAGIC <br>
# MAGIC <br>
# MAGIC You run these workloads as a set of commands in a notebook or as an automated job.
# MAGIC <br>
# MAGIC
# MAGIC
# MAGIC <img src="https://516237376-files.gitbook.io/~/files/v0/b/gitbook-legacy-files/o/assets%2F-MIIWE47MSPMOIxmLgUz%2F-MdGKuZ-zriRADaPpkEi%2F-MdGL-5SaZRHuVHEODP2%2F001-Azure%20Data%20Lake%20Storage%20Credential%20Passthrough.png?alt=media&token=8311d127-558e-4b74-865c-f3af04d15dba" alt="Cluster" width="400"/>
# MAGIC
# MAGIC [Databricks Cluster Docs](https://learn.microsoft.com/en-us/azure/databricks/clusters/)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebooks
# MAGIC A collection of cell that run commands code in a databricks spark cluster <br>
# MAGIC  You can run different languages in a notebook using magic commands <br> 
# MAGIC --------
# MAGIC **Magic commands** overwrite the default language of the notebook 
# MAGIC
# MAGIC <br> 
# MAGIC
# MAGIC >1. %python
# MAGIC >2. %scala
# MAGIC >3. %md
# MAGIC >4. %sql
# MAGIC >5. %r
# MAGIC <br>
# MAGIC
# MAGIC [Azure Databricks Notebook Docs](https://learn.microsoft.com/en-us/azure/databricks/notebooks/)

# COMMAND ----------

# this command cell is a python cell
print('Hola Everyone')

# COMMAND ----------

#%sql
--# Magic commands overwrite the default language of the notebook

SELECT 'this message come from a sql command' AS sql_command

# COMMAND ----------

# When you execute a sql command is stored in a variable called _sqldf
_sqldf.show()

# COMMAND ----------

# MAGIC
# MAGIC %scala
# MAGIC val msg = "this command is running scala language"
# MAGIC print(msg)
# MAGIC // Magic Command to execute scala

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe
# MAGIC
# MAGIC DataFrame is a distributed collection of data organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood.  
# MAGIC <br>
# MAGIC DataFrames can be constructed from a wide array of sources such as structured data files, tables in Hive, external databases, or existing RDDs  
# MAGIC
# MAGIC [DataFrames on Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/getting-started/dataframes-python)

# COMMAND ----------

# We are going to create a dataframe from a list using 'createdataFrame' function

from datetime import datetime, date
df = spark.createDataFrame([
    (1, 2., 'name1', date(2023, 1, 1), datetime(2023, 1, 1, 12, 0)),
    (2, 3., 'name2', date(2023, 2, 1), datetime(2023, 1, 2, 12, 0)),
    (3, 4., 'name3', date(2023, 3, 1), datetime(2023, 1, 3, 12, 0))
], schema='id long, value double, name string, date date, time timestamp')
df.show()

# COMMAND ----------

print(df.dtypes)
#print(df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notation

# COMMAND ----------

# Notations - Dot Notation, Bracket Notation
display(df.select('id'))
# display(df.select('ID'))
# display(df.select(df['id']))
#display(df.select(df.id))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utilities
# MAGIC This module provides various utilities for users to interact with  Databricks. <br>
# MAGIC We are going to focus in the filesystem

# COMMAND ----------

dbutils.help()
#Allow us to run other notebooks inside the current notebook
dbutils.notebook.help()
#Allow us to pass parameter between notebooks or from ADFY
dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## FileSystem
# MAGIC dbutils.fs <br>
# MAGIC Provides utilities for working with FileSystems. <br> 
# MAGIC Most methods in this package can take either a DBFS path (e.g., "/foo" or "dbfs:/foo"), or another FileSystem URI. 

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('ls')

# COMMAND ----------

# Displays information about what is mounted within DBFS, you can see the mountPoint (short) and the source
# Access files using semantics instead of URLs
# Access data without using credentials
# Store files to object storage
display(dbutils.fs.mounts())

# COMMAND ----------

#Lists the contents of a directory -container-
display(dbutils.fs.ls('/databricks-datasets/'))
#display(dbutils.fs.ls('/databricks-datasets/wine-quality/'))


# COMMAND ----------

datasets = dbutils.fs.ls('/databricks-datasets/wine-quality/')
display(datasets)

# COMMAND ----------

disney = dbutils.fs.ls('/mnt/adl2/d3/70_training_dataset_D3/public_datasets/Disney/')
display(disney)


# COMMAND ----------

disney_full = dbutils.fs.ls('abfss://d3-shared-data@cdlprdadl2weu.dfs.core.windows.net/70_training_dataset_D3/public_datasets/Disney/')
display(disney_full)
