from pyspark.sql import *

spark = SparkSession.builder.appName('json_data').getOrCreate()
file1 = spark.read.json('file:///home/ubuntu1/abhilash/example_1.json')

file1.printSchema()