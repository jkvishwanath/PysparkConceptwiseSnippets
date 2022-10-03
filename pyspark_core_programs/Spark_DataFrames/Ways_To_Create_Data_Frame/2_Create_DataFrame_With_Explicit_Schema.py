# Databricks notebook source
from pyspark.sql import SparkSession

spark= SparkSession.builder.master("local").appName("as").getOrCreate()

df1= spark.createDataFrame([
    (1,3,"vishwa","Sw engg"),(3,5,"pavan","Sw Sap")
    ],
    schema='a long,b long,c string,d string'
    )
df1.show()                      
df1.printSchema()
#df1= spark.createDataFrame([Row(,,),Row(,,)]) -- With Rows
#df1= spark.createDataFrame([(,,),(,,)],schema=',,,') --With explicit Schemas