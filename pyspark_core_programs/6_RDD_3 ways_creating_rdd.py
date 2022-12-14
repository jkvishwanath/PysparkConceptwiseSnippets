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
#each nodein cluster has 4 or x no of core processers. Each processor can take one partition.
data = sparkContext.parallelize(a,2)
dataOutput=data.collect()
# Printing output.
for rowdata in dataOutput:
    print(rowdata)

#2nd way of creating rdd. when u read data from file/table using  spark context
#here the no of partitions for rdd will be allocated same as how many unique blocks file takes in hdfs or dbfs. 
#in hdfs each block has 3 replicas. so unique blocks for file it will consider. 
text_file = sc.textFile("dbfs:/FileStore/shared_uploads/vissu4u4ever@gmail.com/2_EmployeeData-2.txt")

#if i specify like below the no of partions = m(no of unique blocks of file in hdfs)* n(number specified below as parameter.)
#text_file = sc.textFile("dbfs:/FileStore/shared_uploads/vissu4u4ever@gmail.com/2_EmployeeData-2.txt",n)


#3rd way of creating rdd. here no of rdds will be same as no of partions when rdd got created in step1 (parallelize) or #stpe2(textFile).
employee_data =text_file.map(lambda line: line.split(",")).map(lambda x: (x[3], int(x[2])))
emp_output= employee_data.collect()
# Printing output.
for data in emp_output:
    print(data)
    
