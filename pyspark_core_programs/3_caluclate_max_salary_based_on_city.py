# Databricks notebook source
# Create SparkSession and sparkcontext
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("local")\
                    .appName('SecondProgram')\
                    .getOrCreate()
sc=spark.sparkContext
# Read the input file and Calculate max sal based on city
text_file = sc.textFile("dbfs:/FileStore/shared_uploads/vissu4u4ever@gmail.com/2_EmployeeData.txt")
employee_data =text_file.map(lambda line: line.split(","))
employee_data.persist()
data=employee_data.map(lambda x: (x[4], int(x[2]))).reduceByKey(lambda x, y: max([x,y]))
output = data.collect()

# Printing maximum salaries city wise
for (city, maxSalary) in output:
    print("%s: %i" % (city, maxSalary))
    


