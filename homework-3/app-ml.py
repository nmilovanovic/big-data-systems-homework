import numpy as np
from pyspark import SparkConf, SparkContext, SQLContext, SparkFiles
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vector
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

#conf = SparkConf().setMaster("spark://localhost:7077").setAppName("MyApp")
conf = SparkConf().setAppName("MyApp")


sc = SparkContext(conf = conf)
sess = SparkSession(sc)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 10)
stream = KafkaUtils.createStream(ssc, "172.17.0.1:2181", "raw-event-streaming-consumer", {"test":1})

#sc.addFile("dataset/trafficData158324.csv")
#rdd1 = sc.textFile("file://"+SparkFiles.get("trafficData158324.csv"))

rdd1 = sc.textFile("hdfs://vm-20172018-009.fsoc.hpi.uni-potsdam.de:9000/apps/nikola.milovanovic/trafficData158324.csv")

#rdd1 = sc.textFile("dataset")

rdd1.cache()

#separate csv by columns
rdd2 = rdd1.map(lambda line: line.split(","))

#extract the following attributes (as PairRDD): reportID, avgMeasuredtime, avgSpeed, vehicleCount
rdd3 = rdd2.map(lambda tup: (tup[8], (tup[1], tup[2], tup[6])))

train_dataset = rdd3.map(lambda x: (3.55*float(x[1][0]) + 6.22*float(x[1][1]), float(x[1][0]), float(x[1][1])))

dataframe = train_dataset.toDF()

assembler = VectorAssembler(inputCols=['_2', '_3'], outputCol = 'features')

dataframe = assembler.transform(dataframe)
dataframe.show(10)

regressor = LinearRegression(featuresCol='features',labelCol = '_1')

#Learn to fit the model from training set
regressor = regressor.fit(dataframe)
print(regressor.coefficients)
print(regressor)

#separate lines
dstream1 = stream.map(lambda x: x[1])

#separate csv by columns
dstream2 = dstream1.map(lambda line: line.split(","))

#extract the following attributes (as PairRDD): reportID, avgMeasuredtime, avgSpeed, vehicleCount
dstream3 = dstream2.map(lambda tup: (float(tup[1]), float(tup[2])))

def operation(rdd):
    if rdd.isEmpty():
        return

    assembler = VectorAssembler(inputCols=['_1', '_2'], outputCol = 'features')
    df = assembler.transform(rdd.toDF())
    x = regressor.transform(df)
    x.show()
    

dstream3.foreachRDD(operation)

ssc.start()
ssc.awaitTermination()