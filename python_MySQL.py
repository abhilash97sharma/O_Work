import pymysql
import pandas as pd
con = pymysql.connect('localhost','root','root','abhi')
emp = pd.read_csv('C:\\Users\\u21a20\\Desktop\\report.csv')
emp.to_sql(con=con,name="report",if_exists="replace")



import pandas as pd
import sqlalchemy
engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost/abhi')
emp = pd.read_csv('C:\\Users\\u21a20\\Desktop\\report.csv')
emp.to_sql('report',con=engine,if_exists="replace")