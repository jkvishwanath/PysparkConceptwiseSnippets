# Databricks notebook source
#https://www.geeksforgeeks.org/defining-dataframe-schema-with-structfield-and-structtype/

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,FloatType,IntegerType,LongType,StringType

spark = SparkSession.builder.master("local").appName("App1").getOrCreate()

input_data = [(("Refrigerator", 112345), 4.0, 12499),
                  (("LED TV", 114567), 4.2, 49999),
                  (("Washing Machine", 113465), 3.9, 69999),
                  (("T-shirt", 124378), 4.1, 1999),
                  (("Jeans", 126754), 3.7, 3999),
                  (("Running Shoes", 134565), 4.7, 1499),
                  (("Face Mask", 145234), 4.6, 999)]
 
# defining schema for the dataframe using
# nested StructType
schm = StructType([
        StructField('Product', StructType([
            StructField('Product Name', StringType(), True),
            StructField('Product ID', LongType(), True),
        ])),
       StructField('Rating', FloatType(), True),
       StructField('Price', IntegerType(), True)])
df1=spark.createDataFrame(input_data,schm)
df1.show()
df1.printSchema()
 
