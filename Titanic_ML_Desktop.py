from pyspark.sql import SQLContext
import re
sqlContext = SQLContext(sc)

file1 = sqlContext.read.csv(r'C:\Users\u21a20\Downloads\titanic3.csv',header = True)
rdd1 = file1.select('name','sex').rdd
file2=file1.filter(f.col('sex').isNotNull())

#for removing a special character from it
rdd2 = rdd1.map(lambda x: re.sub('=+|name|sex|u+|Row','',str(x)))
rdd3 = rdd2.filter(lambda x: re.match('male',x))

survived_titanic1 = survived_titanic.filter(lambda x: x.split(',')[4]=='"male"')
survived_titanic11 = survived_titanic1.flatMap(lambda x: x.split(','))

#for converting a rdd into a dataframe
titanic_df=titanic_rdd.map(lambda x: (x, )).toDF()

#for filtering
rdd112 = rdd1.filter(lambda x: x[1]=='male')
rdd113 = rdd1.filter(lambda x: x[1]=='female')

rdd114 = rdd1.filter(lambda x: re.match('Allen, Miss. Elisabeth Walton',str(x[0])))

for s1 in rdd1.collect():
    s1 = str(s1)
    s1 = re.sub('=+|name|sex|u+','',s1)
	print(s1)