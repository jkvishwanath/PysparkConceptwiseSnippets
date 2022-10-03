# Databricks notebook source
##select gender, sum(sal), count(*), avg(sal), min(sal), max(sal) from profiles group by deptno,gender;
# Create SparkSession and sparkcontext
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("local")\
                    .appName('SecondProgram')\
                    .getOrCreate()
sc=spark.sparkContext
# Read the input file and Calculate total sal based on gender
text_file = sc.textFile("dbfs:/FileStore/shared_uploads/vissu4u4ever@gmail.com/2_EmployeeData-2.txt")
employee_data =text_file.map(lambda line: line.strip().lower().split(",")).map(lambda x: ((x[5],x[3]), int(x[2])))

#using groupbykey may be a bad practice. find alternate best practice 
groupingData=employee_data.groupByKey() 

#adding salaries with sum function.
data=groupingData.map(lambda x: (x[0][0] , x[0][1], sum(x[1]), max(x[1]), min(x[1])  ) )

#adding salaries without max function.
#data=employee_data.reduceByKey(lambda x, y: (x+y))

output = data.collect()

# Printing total salary based on gender
for rowdata in output:
    print(rowdata)
    

    
    
    
