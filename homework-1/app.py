from pyspark import SparkConf, SparkContext, SQLContext

import pyspark

#conf = SparkConf().setMaster("spark://localhost:7077").setAppName("MyApp")
conf = SparkConf().setAppName("MyApp")


sc = SparkContext(conf = conf)

sc.setLogLevel ("ERROR")

#rdd1 = sc.textFile("dataset/trafficData158324.csv")

rdd1 = sc.textFile("dataset")

rdd1.cache()

#separate csv by columns
rdd2 = rdd1.map(lambda line: line.split(","))

#extract the following attributes (as PairRDD): reportID, avgMeasuredtime, avgSpeed, vehicleCount
rdd3 = rdd2.map(lambda tup: (tup[8], (tup[1], tup[2], tup[6])))

#read metadata
rdd4 = sc.textFile("trueMetadata.csv")

#separate csv by columns
rdd5 = rdd4.map(lambda line: line.split(","))

#make PairRDD from csv columns
rdd6 = rdd5.map(lambda tup: (tup[0], (tup[1], tup[2], tup[3], tup[4], tup[5], tup[6], tup[7], tup[8])))

#join two PairRDDs
rddJoined = rdd3.join(rdd6)

rddJoined.cache()
#print(rddJoined.take(2))
gSirinaMin = 56.0

gDuzinaMin = 10.0

gSirinaMax = 57.0

gDuzinaMax = 11.0

#filter by belonging to a rectangle defined with bounds
rddFiltered = rddJoined \
.filter(lambda x: float(x[1][1][1])>gSirinaMin and float(x[1][1][1])<gSirinaMax and float(x[1][1][2])>gDuzinaMin and float(x[1][1][2])<gDuzinaMax)\
.filter(lambda x: float(x[1][1][5])>gSirinaMin and float(x[1][1][5])<gSirinaMax and float(x[1][1][6])>gDuzinaMin and float(x[1][1][6])<gDuzinaMax)

rddFiltered.cache()

#print(rddFiltered.take(10) }

#for each of the attributes, find averages in the rectangle

aggregated1 = rddFiltered.aggregate((0.0,0.0), lambda acc, value: (acc[0]+float(value[1][0][0]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))

aggregated2 = rddFiltered.aggregate((0.0,0.0), lambda acc, value: (acc[0]+float(value[1][0][1]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))

aggregated3 = rddFiltered.aggregate((0.0,0.0), lambda acc, value: (acc[0]+float(value[1][0][2]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))

#result correctness checked in excell

print(aggregated1[0]/aggregated1[1])

print(aggregated2[0]/aggregated2[1])

print(aggregated3[0]/aggregated3[1])

#find min and max values in rectangle, this can be optimized

folded1 = rddFiltered.map(lambda x: float(x[1][0][0])).fold(500, lambda x,y: min(x, y))

folded2 = rddFiltered.map(lambda x: float(x[1][0][1])).fold(500, lambda x,y: min(x, y))

folded3 = rddFiltered.map(lambda x: float(x[1][0][2])).fold(500, lambda x,y: min(x, y))

folded4 = rddFiltered.map(lambda x: float(x[1][0][0])).fold(0, lambda x,y: max(x, y))

folded5 = rddFiltered.map(lambda x: float(x[1][0][1])).fold(0, lambda x,y: max(x, y))

folded6 = rddFiltered.map(lambda x: float(x[1][0][2])).fold(0, lambda x,y: max(x, y))

#result correctness checked in excell

print(folded1, folded2, folded3, folded4, folded5, folded6)

#extract street and city names with appropriate values

rddStreets = rddJoined.filter(lambda x: x[1][1][0] == x[1][1][4]).map(lambda x: (x[1][1][0], x[1][0]))

rddCities = rddJoined.filter(lambda x: x[1][1][3] == x[1][1][7]).map(lambda x: (x[1][1][3], x[1][0]))

rddStreets.cache()

rddCities.cache()

#average, min and max values for streets

rddCombined1 = rddStreets.aggregateByKey((0.0,0.0), lambda acc, value: (acc[0]+float(value[0]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
rddCombined1 = rddCombined1.mapValues(lambda sumcount:sumcount[0]/float(sumcount[1]))

rddCombined2 = rddStreets.aggregateByKey((0.0,0.0), lambda acc, value: (acc[0]+float(value[1]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
rddCombined2 = rddCombined2.mapValues(lambda sumcount:sumcount[0]/float(sumcount[1]))

rddCombined3 = rddStreets.aggregateByKey((0.0,0.0), lambda acc, value: (acc[0]+float(value[2]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
rddCombined3 = rddCombined3.mapValues(lambda sumcount:sumcount[0]/float(sumcount[1]))

print (rddCombined1.collectAsMap(), rddCombined2.collectAsMap(), rddCombined3.collectAsMap())

rddCombined4 = rddStreets.map(lambda x: (x[0],float(x[1][0]))).foldByKey(500, lambda x,y: min(x,y))

rddCombined5 = rddStreets.map(lambda x: (x[0],float(x[1][0]))).foldByKey(0, lambda x,y: max(x,y))

rddCombined6 = rddStreets.map(lambda x: (x[0],float(x[1][1]))).foldByKey(500, lambda x,y: min(x,y))

rddCombined7 = rddStreets.map(lambda x: (x[0],float(x[1][1]))).foldByKey(0, lambda x,y: max(x,y))

rddCombined8 = rddStreets.map(lambda x: (x[0],float(x[1][2]))).foldByKey(500, lambda x,y: min(x,y))

rddCombined9 = rddStreets.map(lambda x: (x[0],float(x[1][2]))).foldByKey(0, lambda x,y: max(x,y))

print(rddCombined4.collectAsMap() , rddCombined5.collectAsMap(), rddCombined6.collectAsMap(), rddCombined7.collectAsMap() , rddCombined8.collectAsMap(), rddCombined9.collectAsMap())

#average, min and max values for cities

rddCombined1 = rddCities.aggregateByKey((0.0,0.0), lambda acc, value: (acc[0]+float(value[0]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
rddCombined1 = rddCombined1.mapValues(lambda sumcount:sumcount[0]/float(sumcount[1]))

rddCombined2 = rddCities.aggregateByKey((0.0,0.0), lambda acc, value: (acc[0]+float(value[1]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
rddCombined2 = rddCombined2.mapValues(lambda sumcount:sumcount[0]/float(sumcount[1]))

rddCombined3 = rddCities.aggregateByKey((0.0,0.0), lambda acc, value: (acc[0]+float(value[2]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
rddCombined3 = rddCombined3.mapValues(lambda sumcount:sumcount[0]/float(sumcount[1]))

print(rddCombined1.collectAsMap(), rddCombined2.collectAsMap(), rddCombined3.collectAsMap())

rddCombined4 = rddCities.map(lambda x:(x[0],float(x[1][0]))).foldByKey(500, lambda x,y: min(x,y))

rddCombined5 = rddCities.map(lambda x:(x[0],float(x[1][0]))).foldByKey(0, lambda x,y: max(x,y))

rddCombined6 = rddCities.map(lambda x:(x[0],float(x[1][1]))).foldByKey(500, lambda x,y: min(x,y))

rddCombined7 = rddCities.map(lambda x:(x[0],float(x[1][1]))).foldByKey(0, lambda x,y: max(x,y))

rddCombined8 = rddCities.map(lambda x:(x[0],float(x[1][2]))).foldByKey(500, lambda x,y: min(x,y))

rddCombined9 = rddCities.map(lambda x:(x[0],float(x[1][2]))).foldByKey(0, lambda x,y: max(x,y))

print(rddCombined4.collectAsMap() , rddCombined5.collectAsMap(), rddCombined6.collectAsMap(), rddCombined7.collectAsMap() , rddCombined8.collectAsMap(), rddCombined9.collectAsMap())