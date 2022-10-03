# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.master("local").appName("dataFrameApp").getOrCreate()

df=spark.createDataFrame([
            Row(a=1,b=2,c="Viswa",d="sw engg"),
            Row(a=6,b=7,c="Pavan",d="Sap consultant")
            ])

df.show()
df.printSchema()

#df1= spark.createDataFrame([Row(,,),Row(,,)])


# COMMAND ----------

