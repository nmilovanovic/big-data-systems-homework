import sys
from pyspark import SparkContext, SparkConf, SparkFiles
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
import uuid

def toAscii(x):
    if x < 128:
        return x
    else:
        return 'X'

cluster = Cluster(['localhost'],port=9042)
session = cluster.connect('test',wait_for_all_pools=True)
session.execute('USE test')

sc = SparkContext(appName="AppName")
sess= SparkSession(sc)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
stream = KafkaUtils.createStream(ssc, "172.17.0.1:2181", "raw-event-streaming-consumer", {"test":1}) 

#separate lines
dstream1 = stream.map(lambda x: x[1])

#separate csv by columns
dstream2 = dstream1.map(lambda line: line.split(","))

#extract the following attributes (as PairRDD): reportID, avgMeasuredtime, avgSpeed, vehicleCount
dstream3 = dstream2.map(lambda tup: (tup[8][:6], (tup[1], tup[2], tup[6]))).filter(lambda x: int(x[1][2])>0)

#read metadata
#sc.addFile("trueMetadata.csv")
#rddMetadata = sc.textFile("file:////home/aleksandra.stojnev/apps/nikola.milovanovic/big-data-projects/spark-project/trueMetadata.csv")
rddMetadata = sc.textFile("hdfs://192.168.42.160:9000/trueMetadata.csv")
#rddMetadata = sc.textFile(SparkFiles.get("trueMetadata.csv"))
#rddMetadata = sc.textFile("trueMetadata.csv")

#separate csv by columns
rddMetadata = rddMetadata.map(lambda line: line.split(","))

#make PairRDD from csv columns
rddMetadata = rddMetadata.map(lambda tup: (tup[0], (tup[1], tup[2], tup[3], tup[4], tup[5], tup[6], tup[7], tup[8])))

#join two PairRDDs
def operation(rdd):
    if rdd.isEmpty():
        return

    rddJoined = rdd.join(rddMetadata)

    #print number of occurences
    count = rddJoined.count()
    print(count)
    session.execute("INSERT INTO test.count (id, val1) VALUES (%s, %s)", (uuid.uuid1(), count))

    #for each of the attributes, find average
    aggregated1 = rddJoined.aggregate((0.0,0.0), lambda acc, value: (acc[0]+float(value[1][0][0]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
    aggregated2 = rddJoined.aggregate((0.0,0.0), lambda acc, value: (acc[0]+float(value[1][0][1]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
    aggregated3 = rddJoined.aggregate((0.0,0.0), lambda acc, value: (acc[0]+float(value[1][0][2]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))

    aggr1 = 0
    aggr2 = 0
    aggr3 = 0
    if aggregated1[1]>0:
        print(aggregated1[0]/aggregated1[1])
        aggr1 = aggregated1[0]/aggregated1[1]
    else:
        print(0)
    if aggregated2[1]>0:
        print(aggregated2[0]/aggregated2[1])
        aggr2 = aggregated2[0]/aggregated2[1]
    else:
        print(0)
    if aggregated3[1]>0:
        print(aggregated3[0]/aggregated3[1])
        aggr3 = aggregated3[0]/aggregated3[1]
    else:
        print(0)

    session.execute("INSERT INTO test.aggregated (id, val1, val2, val3) VALUES (%s, %s, %s, %s)", (uuid.uuid1(), aggr1, aggr2, aggr3))

    #for each of the attributes, find min and max value
    folded1 = rddJoined.map(lambda x: float(x[1][0][0])).fold(500, lambda x,y: min(x, y))
    folded2 = rddJoined.map(lambda x: float(x[1][0][1])).fold(500, lambda x,y: min(x, y))
    folded3 = rddJoined.map(lambda x: float(x[1][0][2])).fold(500, lambda x,y: min(x, y))
    folded4 = rddJoined.map(lambda x: float(x[1][0][0])).fold(0, lambda x,y: max(x, y))
    folded5 = rddJoined.map(lambda x: float(x[1][0][1])).fold(0, lambda x,y: max(x, y))
    folded6 = rddJoined.map(lambda x: float(x[1][0][2])).fold(0, lambda x,y: max(x, y))

    print(folded1, folded2, folded3, folded4, folded5, folded6)
    session.execute("INSERT INTO test.folded (id, val1, val2, val3, val4, val5, val6) VALUES (%s, %s, %s, %s, %s, %s, %s)", (uuid.uuid1(), folded1, folded2, folded3, folded4, folded5, folded6))

    #extract street and city names with appropriate values
    #rddStreets = rddJoined.filter(lambda x: x[1][1][0] == x[1][1][4]).map(lambda x: (''.join(list(map(toAscii,x[1][1][0]))), x[1][0]))
    #rddCities = rddJoined.filter(lambda x: x[1][1][3] == x[1][1][7]).map(lambda x: (''.join(list(map(toAscii,x[1][1][3]))), x[1][0]))
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

    map1 = rddCombined1.collectAsMap()
    map2 = rddCombined2.collectAsMap()
    map3 = rddCombined3.collectAsMap()
    print(map1, map2, map3)
    for key in map1.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map1[key]))
    for key in map2.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map2[key]))
    for key in map3.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map3[key]))

    rddCombined4 = rddStreets.map(lambda x: (x[0],float(x[1][0]))).foldByKey(500, lambda x,y: min(x,y))
    rddCombined5 = rddStreets.map(lambda x: (x[0],float(x[1][0]))).foldByKey(0, lambda x,y: max(x,y))
    rddCombined6 = rddStreets.map(lambda x: (x[0],float(x[1][1]))).foldByKey(500, lambda x,y: min(x,y))
    rddCombined7 = rddStreets.map(lambda x: (x[0],float(x[1][1]))).foldByKey(0, lambda x,y: max(x,y))
    rddCombined8 = rddStreets.map(lambda x: (x[0],float(x[1][2]))).foldByKey(500, lambda x,y: min(x,y))
    rddCombined9 = rddStreets.map(lambda x: (x[0],float(x[1][2]))).foldByKey(0, lambda x,y: max(x,y))

    map4 = rddCombined4.collectAsMap()
    map5 = rddCombined5.collectAsMap()
    map6 = rddCombined6.collectAsMap()
    map7 = rddCombined7.collectAsMap()
    map8 = rddCombined8.collectAsMap()
    map9 = rddCombined9.collectAsMap()
    print(map4, map5, map6, map7, map8, map9)
    for key in map4.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map4[key]))
    for key in map5.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map5[key]))
    for key in map6.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map6[key]))
    for key in map7.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map7[key]))
    for key in map8.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map8[key]))
    for key in map9.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map9[key]))

    if not rddCombined9.isEmpty():
        print(rddCombined9.toDF().orderBy("_2", ascending=False).rdd.collect())
        #df1.show(10)
    
    #average, min and max values for cities
    rddCombined1 = rddCities.aggregateByKey((0.0,0.0), lambda acc, value: (acc[0]+float(value[0]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
    rddCombined1 = rddCombined1.mapValues(lambda sumcount:sumcount[0]/float(sumcount[1]))
    rddCombined2 = rddCities.aggregateByKey((0.0,0.0), lambda acc, value: (acc[0]+float(value[1]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
    rddCombined2 = rddCombined2.mapValues(lambda sumcount:sumcount[0]/float(sumcount[1]))
    rddCombined3 = rddCities.aggregateByKey((0.0,0.0), lambda acc, value: (acc[0]+float(value[2]), acc[1]+1), lambda acc1, acc2: (acc1[0]+acc2[0], acc1[1]+acc2[1]))
    rddCombined3 = rddCombined3.mapValues(lambda sumcount:sumcount[0]/float(sumcount[1]))

    map1 = rddCombined1.collectAsMap()
    map2 = rddCombined2.collectAsMap()
    map3 = rddCombined3.collectAsMap()
    print(map1, map2, map3)
    for key in map1.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map1[key]))
    for key in map2.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map2[key]))
    for key in map3.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map3[key]))

    rddCombined4 = rddCities.map(lambda x:(x[0],float(x[1][0]))).foldByKey(500, lambda x,y: min(x,y))
    rddCombined5 = rddCities.map(lambda x:(x[0],float(x[1][0]))).foldByKey(0, lambda x,y: max(x,y))
    rddCombined6 = rddCities.map(lambda x:(x[0],float(x[1][1]))).foldByKey(500, lambda x,y: min(x,y))
    rddCombined7 = rddCities.map(lambda x:(x[0],float(x[1][1]))).foldByKey(0, lambda x,y: max(x,y))
    rddCombined8 = rddCities.map(lambda x:(x[0],float(x[1][2]))).foldByKey(500, lambda x,y: min(x,y))
    rddCombined9 = rddCities.map(lambda x:(x[0],float(x[1][2]))).foldByKey(0, lambda x,y: max(x,y))

    map4 = rddCombined4.collectAsMap()
    map5 = rddCombined5.collectAsMap()
    map6 = rddCombined6.collectAsMap()
    map7 = rddCombined7.collectAsMap()
    map8 = rddCombined8.collectAsMap()
    map9 = rddCombined9.collectAsMap()
    print(map4, map5, map6, map7, map8, map9)
    for key in map4.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map4[key]))
    for key in map5.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map5[key]))
    for key in map6.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map6[key]))
    for key in map7.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map7[key]))
    for key in map8.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map8[key]))
    for key in map9.keys():
        session.execute("INSERT INTO test.combined (id, val1, val2) VALUES (%s, %s, %s)", (uuid.uuid1(), key, map9[key]))

    if not rddCombined9.isEmpty():
        print(rddCombined9.toDF().orderBy("_2", ascending=False).rdd.collect())
        #df1.show(10)
    

dstream3.foreachRDD(operation)

ssc.start()
ssc.awaitTermination()