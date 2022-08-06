# Databricks notebook source
##select gender, sum(sal) from profiles group by gender;
# Create SparkSession and sparkcontext
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("local")\
                    .appName('SecondProgram')\
                    .getOrCreate()
sc=spark.sparkContext
# Read the input file and Calculate total sal based on gender
text_file = sc.textFile("dbfs:/FileStore/shared_uploads/vissu4u4ever@gmail.com/2_EmployeeData-1.txt")
employee_data =text_file.map(lambda line: line.split(",")).map(lambda x: (x[3], int(x[2])))

# To persist rdd.
employee_data.persist()

#adding salaries with sum function.
sumSal=employee_data.reduceByKey(lambda x, y: sum([x,y]) )
maxSal=employee_data.reduceByKey(lambda x, y: max([x,y]) )
minSal=employee_data.reduceByKey(lambda x, y: min([x,y]) )


sumSal.saveAsTextFile("sumSal5.txt")
maxSal.saveAsTextFile("maxSal5.txt")
output=minSal.collect()
employee_data.unpersist()


# Printing total salary based on gender
for ( rowdata) in output:
    print(rowdata)
    

    
