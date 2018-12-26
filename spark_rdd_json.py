from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
polygon = Polygon([[1,1],[-1,1],[-1,-1],[1,-1]])
point = Point(0.5,0.5)



rdd1.take(1)[0].coordinates[5][0]
rdd12.take(1)[0].type
rdd1.take(1)[0].features[0].geometry
rdd1.take(1)[0].features[0].geometry.coordinates
rdd1.take(1)[0].features[0].geometry.coordinates[0][1]

for i in range(1,10):
    rdd1.take(1)[0].features[i].geometry.coordinates[0]
	

#-------------------------------

for i in range(0,len(lst1)):
       polygon = Polygon(lst1[i])
       rdd2 = rdd1.map(lambda x:Point(x.device_location_lat,x.device_location_lng).within(polygon))

rdd2.take(3)	

rdd.saveAsTextFile('file:///home/ubuntu1/abhilash/polygon')
file2 = file1.withColumn("status",