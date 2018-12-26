from pyspark import SparkContext
from pyspark import SQLContext
import pyspark.sql.functions as f
import pandas
import matplotlib.pyplot as plt

sc = SparkContext('local','Movie_Lends dataset')
sqlContext = SQLContext(sc)
file1 = sqlContext.read.csv('file:///home/ubuntu1/abhilash/ml-1m/movies.dat')
file2 = sqlContext.read.csv('file:///home/ubuntu1/abhilash/ml-1m/ratings.dat')
file3 = sqlContext.read.csv('file:///home/ubuntu1/abhilash/ml-1m/users.dat')
split_col1 = f.split(file1['_c0'],'::')
file1 = file1.withColumn('MovieID',split_col1.getItem(0))
file1 = file1.withColumn('Title',split_col1.getItem(1))
file1 = file1.withColumn('Genres',split_col1.getItem(2)).drop('_c0')
#file1.show()

split_col2 = f.split(file2['_c0'],'::')
file2 = file2.withColumn('UserID',split_col2.getItem(0))
file2 = file2.withColumn('MovieID',split_col2.getItem(1))
file2 = file2.withColumn('Rating',split_col2.getItem(2))
file2 = file2.withColumn('Timestamp1',split_col2.getItem(3)).drop('_c0')
file2 = file2.withColumn('Timestamp',f.from_unixtime('Timestamp1', 'yyyy-MM-dd HH:MM:SS')).drop('Timestamp1')
#file2.show()
#file2.printSchema()

split_col3 = f.split(file3['_c0'],'::')
file3 = file3.withColumn('UserID',split_col3.getItem(0))
file3 = file3.withColumn('Gender',split_col3.getItem(1))
file3 = file3.withColumn('Age',split_col3.getItem(2))
file3 = file3.withColumn('Occupation',split_col3.getItem(3))
file3 = file3.withColumn('Zip-code',split_col3.getItem(4)).drop('_c0')
#file3.show()

#file4 = file1.join(file2, file1.MovieID == file2.MovieID,"inner")  #alternate of it.
file4 = file1.join(file2, ['MovieID'] ,"inner")
file5=file4.na.drop()

split_date = f.split(file5['Timestamp'],' ')
file6=file5.withColumn('Date',split_date.getItem(0).cast('date')).withColumn('Time',split_date.getItem(1))
file6=file6.withColumn('Year',f.year(f.col('Date'))).withColumn('month',f.month(f.col('Date'))).withColumn('day',f.dayofmonth(f.col('Date'))).drop('Timestamp')
#print(file5.count())
#print(file4.count())

#......module1
f1=file5.groupBy('MovieID','Rating','Title').count()  #how many times which movie get what rating 
#for i in f1.take(10):
#	print(i)
#f1.orderBy(f.desc('Rating'),f.desc('count')).show(10,truncate=False)
f1=f1.orderBy(f.desc('Rating'),f.desc('count')).limit(10).toPandas()
print('converted into pandas')
#f2=f1.orderBy(f.desc('Rating'),f.desc('count')).toPandas()
plt.rcParams["figure.figsize"] = [30,25]
f1.plot(x="Title",y='count',kind="bar") 
plt.savefig('/home/ubuntu1/abhilash/spark/Top_rt_mov.png',dpi=200)

#fl=file5.groupBy('Genres').count()
#fl.orderBy(f.desc('count')).show()
#file4.show(truncate=False)

#........

#.......module2
#file5.show(truncate=False)
month_lst = ['January', 'Feburary', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
def mnth(x):
	m=month_lst[x-1]
	return(m)

fun1 = f.udf(mnth)	 
f2=file6.withColumn('month1',fun1(f.col('month')))
f2=f2.groupBy('month1').count()
f2=f2.orderBy(f.desc('count')).toPandas()
#f2.show(truncate=False)
f2.plot(x="month1",y='count',kind="bar") 
plt.savefig('/home/ubuntu1/abhilash/spark/Top_month_wat_mov.png',dpi=200)
#........

#......module3
f3=file6.groupBy('Year').count()
f3=f3.orderBy(f.desc('count')).toPandas()
f3.plot(x="Year",y='count',kind="bar") 
plt.savefig('/home/ubuntu1/abhilash/spark/No_of_mov_year.png',dpi=200)

#.....module4
f4=file6.filter(f.col('Rating')==5)
f4=f4.groupBy('UserID').count().limit(10)
f4=f4.orderBy(f.desc('count')).toPandas()
f4.plot(x="UserID",y='count',kind="bar") 
plt.savefig('/home/ubuntu1/abhilash/spark/rat_five_usr.png',dpi=200)
#f4.printSchema()
#f4.show(truncate=False)
#.......

#.....module5
f5=file6.filter(f.col('Rating')==4)
f5=f5.groupBy('UserID').count().limit(10)
f5=f5.orderBy(f.desc('count')).toPandas()
f5.plot(x="UserID",y='count',kind="bar") 
plt.savefig('/home/ubuntu1/abhilash/spark/rat_four_usr.png',dpi=200)

#.....module6
f6=file6.filter(f.col('Rating')==3)
f6=f6.groupBy('UserID').count().limit(10)
f6=f6.orderBy(f.desc('count')).toPandas()
f6.plot(x="UserID",y='count',kind="bar") 
plt.savefig('/home/ubuntu1/abhilash/spark/rat_three_usr.png',dpi=200)

#.....module7
f7=file6.filter(f.col('Rating')==2)
f7=f7.groupBy('UserID').count().limit(10)
f7=f7.orderBy(f.desc('count')).toPandas()
f7.plot(x="UserID",y='count',kind="bar") 
plt.savefig('/home/ubuntu1/abhilash/spark/rat_two_usr.png',dpi=200)

#.....module8
f8=file6.filter(f.col('Rating')==1)
f8=f8.groupBy('UserID').count().limit(10)
f8=f8.orderBy(f.desc('count')).toPandas()
f8.plot(x="UserID",y='count',kind="bar") 
plt.savefig('/home/ubuntu1/abhilash/spark/rat_one_usr.png',dpi=200)

#.....module9
f9=file6.groupBy('UserID').count().limit(10)
f9=f9.orderBy(f.desc('count')).toPandas()
f9.plot(x="UserID",y='count',kind="bar") 
plt.savefig('/home/ubuntu1/abhilash/spark/top_rat_usr.png',dpi=200)

