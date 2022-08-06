# Databricks notebook source
# Create SparkSession and sparkcontext
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("local")\
                    .appName('SecondProgram')\
                    .getOrCreate()
sc=spark.sparkContext
# Read the input file and Calculate total sal based on gender
text_file = sc.textFile("dbfs:/FileStore/shared_uploads/vissu4u4ever@gmail.com/2_EmployeeData.txt")
data =text_file.map(lambda line: line.split(",")).map(lambda x: (x[3], int(x[2]))).reduceByKey(lambda x, y: x + y)
output = data.collect()

# Printing total salary based on gender
for (gender, totalSalary) in output:
    print("%s: %i" % (gender, totalSalary))
    


# COMMAND ----------


counts = text_file.map(lambda line: line.split(",")) \
                           .map(lambda rowData: (rowData[4], rowData[3])) \
                           .reduceByKey(lambda x, y: x + y)                                                         

# Printing each word with its respective count
output = counts.collect()
for (word, count) in output:
    print(word)
