# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("as").getOrCreate()
rdd1=spark.sparkContext.parallelize([(1,4,"viswa","hari"),(3,4,"Hari","viswa")])
df1=spark.createDataFrame(rdd1,schema=['a','b','c','d'])
df1.show()

df1.printSchema()


#df1= spark.createDataFrame([Row(,,),Row(,,)]) -- With Rows
#df1= spark.createDataFrame([(,,),(,,)],schema=',,,') --With explicit Schemas

#rdd1=spark.sparkContext.parallelize([(,,),(,,)])
#df1= spark.createDataFrame(rdd1,schema=[,,,])  --with Rdd



