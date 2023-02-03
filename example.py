# Copyright 2019 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# # Example Python Code for Cloudera Data Science Workbench Training

# ## Basics

# In a Python script in CDSW, include comments and code
# like you would in any other Python script.

print("Hello world!")

1 + 1

# When you run part or all of the script, the comments,
# code, and output are displayed in the session console.

# CDSW supports Jupyter magic commands. For example, you
# can use the shell magic to issue operating system
# commands:

!ls -l

# For more details, see
# [the documentation on Jupyter magic commands](https://www.cloudera.com/documentation/data-science-workbench/latest/topics/cdsw_jupyter.html).


# ## Markdown

# Comments in a code file in CDSW can include
# [Markdown](https://daringfireball.net/projects/markdown/syntax).
# For example:

# # Heading 1

# ## Heading 2

# ### Heading 3

# Plain text

# *Emphasized text*

# **Bold text**

# `Monospaced text`

# Bulleted list
# * Item
#   * Subitem
# * Item

# Numbered list
# 1. First Item
# 2. Second Item
# 3. Third Item

# [link](https://www.cloudera.com)


# ## Copying Files to HDFS

# This project includes a dataset describing on-time
# performance for flights departing New York City airports
# (EWR, JFK, and LGA) in the year 2013. This data was
# collected by the U.S. Department of Transportation. It
# is stored here in a comma-separated values (CSV) file
# named `flights.csv`.

# Copy this file to HDFS by running `hdfs dfs` commands in
# CDSW using shell magic:

# Delete the `flights` subdirectory and its contents in
# your home directory, in case it already exists:

!hdfs dfs -rm -r flights

# Create the `flights` subdirectory:

!hdfs dfs -mkdir flights

# Copy the file into it:

!hdfs dfs -put flights.csv flights/

# The file `flights.csv` is now stored in the subdirectory
# `flights` in your home directory in HDFS.


# ## Using Apache Spark 2 with PySpark

# CDSW provides a virtual gateway to the cluster, which
# you can use to run Apache Spark jobs using PySpark,
# Spark's Python API.

# Before you connect to Spark: If you are using a secure
# cluster with Kerberos authentication, you must first go
# to the Hadoop Authentication section of your CDSW user
# settings and enter your Kerberos principal and password.


# ### Connecting to Spark

# Spark SQL is Spark's module for working with structured
# data. PySpark is Spark's Python API. The `pyspark.sql`
# module exposes Spark SQL functionality to Python.

# Begin by importing `SparkSession`, PySpark's main entry
# point:

from pyspark.sql import SparkSession

# Then call the `getOrCreate()` method of
# `SparkSession.builder` to connect to Spark. This
# example connects to Spark on YARN and gives a name to
# the Spark application:

spark = SparkSession.builder \
  .master("yarn") \
  .appName("cdsw-training") \
  .getOrCreate()

# Now you can use the `SparkSession` named `spark` to read
# data into Spark.


# ### Reading Data

# Read the flights dataset from HDFS. This data is in CSV
# format and includes a header row. Spark can infer the
# schema automatically from the data:

flights = spark.read.csv("flights/", header=True, inferSchema=True)

# The result is a Spark DataFrame named `flights`.


# ### Inspecting Data

# Inspect the DataFrame to gain a basic understanding
# of its structure and contents.

# Print the number of rows:

flights.count()

# Print the schema:

flights.printSchema()

# Inspect one or more variables (columns):

flights.describe("arr_delay").show()
flights.describe("arr_delay", "dep_delay").show()

# Print the first five rows:

flights.limit(5).show()

# Or more concisely:

flights.show(5)

# Print the first 20 rows (the default number is 20):

flights.show()

# `show()` can cause rows to wrap onto multiple lines,
# making the output hard to read. To make the output
# more readable, use `toPandas()` to return a pandas
# DataFrame. For example, return the first five rows
# as a pandas DataFrame and display it:

#
# To display the pandas DataFrame in a scrollable
# grid, import pandas and set the pandas option
# `display.html.table_schema` to `True`:

#import pandas as pd
#pd.set_option("display.html.table_schema", True)

#flights_pd

# Caution: When working with a large Spark DataFrame,
# limit the number of rows before returning a pandas
# DataFrame.


# ### Transforming Data

# Spark SQL provides a set of functions for manipulating
# Spark DataFrames. Each of these methods returns a
# new DataFrame.

# `select()` returns the specified columns:

#flights.select("carrier").show()

# `distinct()` returns distinct rows:

#flights.select("carrier").distinct().show()

# `filter()` (or its alias `where()`) returns rows that
# satisfy a Boolean expression.

# To disambiguate column names and literal strings,
# import and use the functions `col()` and `lit()`:

#ghts.filter(col("dest") == lit("SFO")).show()

# `orderBy()` (or its alias `sort()`) returns rows
# arranged by the specified columns:

#
spark.stop()
