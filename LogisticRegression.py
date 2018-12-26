from pyspark.sql import SQLContext
from pyspark import SparkContext

sc = SparkContext('local','LogisticRegression')
sqlContext = SQLContext(sc)


file1=sqlContext.read.format('com.crealytics.spark.excel').option('location','/home/ubuntu1/abhilash/files/titanic3.xls').option('useHeader','true').option('treatEmptyValuesAsNulls','true').option('inferSchema','true').option('addColorColumns','False').load() 

#file1.show(5,truncate=False)
file1 = file1.withColumnRenamed('home.dest','homedest')
file1.printSchema()
#file1.select('pclass').distinct().show()   #for showing distinct values in the pclass columns
#file1.groupBy('pclass').count().show()    # for showing the count of the pclass classes in it