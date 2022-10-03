# Databricks notebook source
#https://www.geeksforgeeks.org/pyspark-create-dataframe-from-list/
#https://www.geeksforgeeks.org/create-pyspark-dataframe-from-list-of-tuples/
#https://www.geeksforgeeks.org/create-pyspark-dataframe-from-dictionary/

# importing sparksession from 
# pyspark.sql module
from pyspark.sql import SparkSession
  
# creating sparksession and giving 
# an app name
spark = SparkSession.builder.appName('sparkdf').getOrCreate()
  
# list  of college data with two lists
data = [["java", "dbms", "python"], 
        ["OOPS", "SQL", "Machine Learning"]]

columnNames=["subject1","subject2","subject3"]

df1=spark.createDataFrame(data,columnNames)

df1.show()
df1.printSchema()




#list of tuples of plants data
tuple_data2 = [("mango", "AP", "Guntur"),
        ("mango", "AP", "Chittor"),
        ("sugar cane", "AP", "amaravathi"),
        ("paddy", "TS", "adilabad"),
        ("wheat", "AP", "nellore")]
  
# giving column names of dataframe
columnNames2 = ["Crop Name", "State", "District"]
  
# creating a dataframe 
df2 = spark.createDataFrame(tuple_data2, columnNames2)
df2.show()
df2.printSchema()


# with three  dictionaries
studentDictionaryData = [{'student_id': 12, 'name': 'sravan', 'address': 'kakumanu'},
        {'student_id': 14, 'name': 'jyothika', 'address': 'tenali'},
        {'student_id': 11, 'name': 'deepika', 'address': 'repalle'}]
  
# creating a dataframe
df3=spark.createDataFrame(studentDictionaryData)
  
# show data frame
df3.show()
df3.printSchema()
