import csv
import sys
import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark.sql.functions import *

#CommandLine Args
dataDirectory = sys.argv[1]
schema_file = sys.argv[2]
bootstrap_servers = sys.argv[3]
topic = sys.argv[4]

#Constants
spark = None
ssc = None
float_fields = [157]
datetime_fields = [158]


def createSchema():
    global schema_str

    schema_str = open(schema_file, 'r').read().strip('\n')
    headers = schema_str.split(',')
    fields = [StructField(ele, StringType(), True) for ele in headers]
    fields[157].dataType = FloatType()
    fields[158].dataType = TimestampType()
    schema = StructType(fields)

    return schema


def process():
        
    def writeInDf(rdd):

        def createTuple(istr):
            possible_yrs = [2013,2014,2015,2016,2017]
            jl = ()
            for i, e in enumerate(csv.reader(istr.split(','))):
                if i in float_fields+datetime_fields:
                    if e:
                        e = e[0]
                        if i in float_fields:
                            jl += (float(e),)
                        elif i in datetime_fields:
                            if '-' in e:
                                tl = e.split('-')
                                (y, m, d) = (int(tl[2]),int(tl[1]),int(tl[0]))
                            else:
                                tl = e.strip('"').split('/')
                                (y, m, d) = (int(tl[2]),int(tl[0]),int(tl[1]))
                            if y not in possible_yrs:
                                (y,m,d) = (2015,1,1)
                            jl += (datetime.datetime(y,m,d),)
                    else:
                        jl += (None,)
                elif e:
                    jl += (e[0],)
                else:
                    jl += (str(),)
            return jl

        headerLessRdd = rdd.filter(lambda x: schema_str not in x)
        tupleRdd = headerLessRdd.map(createTuple)
        schema = createSchema()
        df = spark.createDataFrame(tupleRdd, schema)
        df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).option("topic", topic).save()
        
    fileDstream = ssc.textFileStream(dataDirectory)
    fileDstream.foreachRDD(writeInDf)

    ssc.start()
    ssc.awaitTermination()


def main():

    global spark
    global ssc

    sc = SparkContext(conf=SparkConf().setAppName("FileDStreamToDF"))
    ssc = StreamingContext(sc,10)
    spark = SparkSession.builder.appName("FileDStreamToDF").getOrCreate()
    process()


if __name__ == '__main__':
    main()




