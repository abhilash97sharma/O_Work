from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext('local[*]','Latapp')
sqlContext = SQLContext(sc)

file1 = sqlContext.read.csv('file:///home/ubuntu1/husain/nikaza/user_data/us/*.csv.gz')
file1.count()

file1.show()