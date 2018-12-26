from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import pandas

def time_delta(y, x):        
    if (y == "null" or x == "null"):
        return "null"
    else:
        from datetime import datetime
        end = datetime.strptime(y, '%Y-%m-%d %H:%M:%S')
        start = datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        delta = (start-end).total_seconds()
        return delta
# register as a UDF 
f1 = f.udf(time_delta)

def time_sub(y,x):
    z=y-x
    return z

f2 = f.udf(time_sub)

sc = SparkContext('local','MySQL_UC1')
sqlContext = SQLContext(sc)
device=sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/husain?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable","device").option("user","root").option("password","root").load()
emp=sqlContext.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/husain?useSSL=false").option("driver", "com.mysql.jdbc.Driver").option("dbtable","employee").option("user","root").option("password","root").load()
emp = emp.withColumn('Timings',f.rtrim(emp.timings))
emp=emp.join(device,device.device_id == emp.device_id,"inner").select(emp.id,emp.name,emp.device_id,device.status,emp.Timings)
split_col = f.split(emp['Timings'],' ')
emp = emp.withColumn('Date',split_col.getItem(0).cast('date'))
#emp = emp.orderBy('Date','name')
#emp.show()
emp1 = emp.filter(emp['status']=='in')
emp1=emp1.orderBy('Date','name','Timings')
emp2 = emp.filter(emp.status=='out')
emp2 = emp2.orderBy('Date','name','Timings')
#
emp1 = emp1.withColumnRenamed('Timings','IN_TIME')
emp2 = emp2.withColumnRenamed('Timings','OUT_TIME')
#
#emp1=emp1.join(emp2,emp1.id == emp2.id,"full_outer").select(emp1.id,emp1.Date,emp1.name,emp1.device_id,emp1.status,emp1.IN_TIME,emp2.OUT_TIME)
#emp1 = emp1.
#emp1 = emp1.withColumn("In_Office",f1(emp1['IN_TIME'],emp1['OUT_TIME']).cast('int'))
emp1=emp1.withColumn("row_number", f.row_number().over(Window.orderBy("IN_TIME")))
emp2=emp2.withColumn("row_number", f.row_number().over(Window.orderBy("OUT_TIME")))
emp3=emp1.join(emp2,emp1.row_number == emp2.row_number,"inner").select(emp1.id,emp1.Date,emp1.name,emp1.IN_TIME,emp2.OUT_TIME).withColumn('IN_OFFICE',f1(emp1.IN_TIME,emp2.OUT_TIME).cast('float'))
#emp3.show()

#....... in progress

emp3=emp3.withColumn("In_time_sec",f.unix_timestamp('IN_TIME'))
emp3=emp3.withColumn("Out_time_sec",f.unix_timestamp('OUT_TIME'))
emp31=emp3.groupBy('name','Date','id').min('In_time_sec')
emp31=emp31.withColumn('rownum',f.row_number().over(Window.orderBy("min(In_time_sec)")))
emp32=emp3.groupBy('name','Date','id').max('Out_time_sec')
emp32=emp32.withColumn('rownum',f.row_number().over(Window.orderBy("max(Out_time_sec)")))
emp31=emp31.withColumnRenamed('min(In_time_sec)','min_time')
emp32=emp32.withColumnRenamed('max(Out_time_sec)','max_time')
#emp31.show()
emp33 = emp31.join(emp32, ['rownum'] ,"inner").select(emp31.id,emp31.name,emp31.Date,emp32.max_time,emp31.min_time)
emp33=emp33.withColumn('min_time1',f.from_unixtime('min_time','yyyy-MM-dd HH:MM:SS')).drop('min_time')
emp33=emp33.withColumn('max_time1',f.from_unixtime('max_time','yyyy-MM-dd HH:MM:SS')).drop('max_time')
emp33=emp33.withColumnRenamed('min_time1','min_time')
emp33=emp33.withColumnRenamed('max_time1','max_time')
emp33=emp33.withColumn('Total_hours',f1(emp33.min_time,emp33.max_time).cast('float'))  #showing total hours worked
emp33=emp33.withColumn('rownum',f.row_number().over(Window.orderBy("id")))
#emp33.show()

#emp31=emp3.groupBy('name','Date').min('IN_TIME')      #not working
#emp31=emp3.withColumn('In_Office',f.col('IN_OFFICE').cast('timeStamp'))
#emp31.select(f.col('IN_TIME')).first.getString(0).show()
#emp31.printSchema()
#emp31.show()

#...........

emp4=emp3.groupBy('id','name','Date').sum('IN_OFFICE')
emp4=emp4.withColumnRenamed('sum(IN_OFFICE)','IN_OFFICE1')
#emp4 = emp4.withColumn("IN_OFFICE(min)",f.col('IN_OFFICE1')/(60*60)).drop('IN_OFFICE1')
emp4=emp4.withColumn('rownum',f.row_number().over(Window.orderBy("id")))
emp4=emp4.join(emp33, ['rownum'] ,"inner").select(emp33.id,emp33.name,emp33.Date,emp4.IN_OFFICE1,emp33.Total_hours)
emp4=emp4.withColumn('Out_hours',f2(emp4.Total_hours,emp4.IN_OFFICE1))
emp4.show()
#emp5=emp4.toPandas()   #for changing the spark dataframe into the pandas dataframe
#emp5.to_csv('/home/ubuntu1/report.csv')      #using the pandas dataframe to write into the csv format
#1emp4.printSchema()
#1emp4.write.format("csv").save("file:///home/ubuntu1/report.csv")

#emp2.show()
#emp = emp.orderBy('Date','name')
#my_window = Window.partitionBy().orderBy("Timings")
#emp = emp.withColumn("prev_value", f.lag(emp.Timings).over(my_window))
#emp.groupBy('Date','id').count().show()
#emp = emp.na.fill("2018-08-22 09:30:00")
#emp = emp.withColumn("diff",f1(emp.prev_value,emp.Timings).cast('int'))

#device.show()
#emp.show()
#device.printSchema()
#emp.printSchema()
#device.join(emp,device.device_id == emp.device_id,"inner").show()
#device.join(emp,device.device_id == emp.device_id,"left_outer").show()
#device.join(emp,device.device_id == emp.device_id,"right_outer").show()
#device.join(emp,device.device_id == emp.device_id,"full_outer").show()