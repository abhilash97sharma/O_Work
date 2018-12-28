from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("yarn").setAppName("test")
sc = SparkContext(conf = conf)
sc.setLogLevel("WARN")

schema1=StructType([StructField('ID',StringType(),True),StructField('SAP_CS_Product',StringType(),True),StructField('Case_Number',StringType(),True),StructField('Account_Name_Site',StringType(),True),StructField('Date_Time_Opened',StringType(),True),StructField('Date_Closed',StringType(),True),StructField('Case_Owner',StringType(),True),StructField('ITS_Service_ Organization',StringType(),True),StructField('Case_Owner_Company',StringType(),True),StructField('ITS_Market_(Affiliate)',StringType(),True),StructField('Serial_ Number',StringType(),True),StructField('Material',StringType(),True),StructField('Material_Number',StringType(),True),StructField('Equipment',StringType(),True),StructField('Equipment_No.',StringType(),True),StructField('Type_of_GAS_PEMS',StringType(),True),StructField('Subject_(English)',StringType(),True),StructField('Description_(English)',StringType(),True),StructField('Analysis_Steps_and_Results',StringType(),True),StructField('Workaround_Description',StringType(),True),StructField('Solution_Proposal',StringType(),True),StructField('Case_Solution_Description',StringType(),True),StructField('language',StringType(),True),StructField('issue',StringType(),True),StructField('issue_des',StringType(),True),StructField('class',StringType(),True),StructField('component',StringType(),True),StructField('condition',StringType(),True),StructField('error',StringType(),True),StructField('Opened.Date',StringType(),True),StructField('Opened.month',StringType(),True),StructField('Opened.year',StringType(),True),StructField('days.open',StringType(),True),StructField('category',StringType(),True)])

spark = SparkSession.builder.appName('structure_streaming').getOrCreate()
df1 = spark.readStream.format('csv').option('header','true').option('maxFilesPerTrigger',1).schema(schema1).load('/user/user1/abhilash/data/')

df2=df1.groupBy('SAP_CS_Product').count()

#query = df2.writeStream.outputMode('complete').format('console').start()
query = df2.writeStream.queryName('ex').format('csv').outputMode('complete').option('path','/user/user1/abhilash/data1').option('checkpointLocation','/user/user1/abhilash/data1/').start()
query.awaitTermination()