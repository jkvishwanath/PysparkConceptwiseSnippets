# Databricks notebook source
# 3 ways to create rdd

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
sparkContext=spark.sparkContext

#a is a list and local object in single system.
a= [10,20,30,40,50]


#1st way of creating rdd
#create a data rdd with 2 partitions. 
data = sparkContext.parallelize(a,2)
dataOutput=data.collect()
# Printing output.
for rowdata in dataOutput:
    print(rowdata)

#2nd way of creating rdd. when u read data from file/table using  spark context
#here the no of partitions for rdd will be allocated same as how many partitions file takes in hdfs or dbfs.
text_file = sc.textFile("dbfs:/FileStore/shared_uploads/vissu4u4ever@gmail.com/2_EmployeeData-2.txt")

#3rd way of creating rdd.
employee_data =text_file.map(lambda line: line.split(",")).map(lambda x: (x[3], int(x[2])))
emp_output= employee_data.collect()
# Printing output.
for data in emp_output:
    print(data)
    
