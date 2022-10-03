# Databricks notebook source
#https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe/
from pyspark.sql import SparkSession
import pandas as pd

spark= SparkSession.builder.master("local").appName("as").getOrCreate()


#df1=spark.read.option("header",True).option("inferSchema",True).csv("dbfs:/FileStore/shared_uploads/vissu4u4ever@gmail.com/train_dataset_1.csv")
df1=spark.read.options(header="True",inferSchema="True",delimiter=",").csv("dbfs:/FileStore/shared_uploads/vissu4u4ever@gmail.com/train_dataset_1.csv")

# inferSchema maintains the column data types as it is from source.
# if inferSchema is n not specified then all column data types are treated as String

df1.show()
df1.printSchema()



# COMMAND ----------

