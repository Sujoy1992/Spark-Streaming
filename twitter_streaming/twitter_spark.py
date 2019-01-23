import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


bootstrap_servers = sys.argv[1]
topic = sys.argv[2]
schema_file = sys.argv[3]
output_path = sys.argv[4]
checkpoint_directory = sys.argv[5]

def main():
    # Creating the spark session
    spark = SparkSession.builder.appName('twitter kafka spark structured stream').getOrCreate()

    # Creating kafka structured stream
    df = spark.readStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers", bootstrap_servers) \
              .option("subscribe", topic) \
              .load()

    # Preparing the schema
    schema_str = open(schema_file, 'r').read().strip('\n')

    headers = schema_str.split(',')
    fields = [StructField(ele, StringType(), True) for ele in headers]
    fields[0].dataType = TimestampType()
    fields[4].dataType = IntegerType()
    schema = StructType(fields)

    # Transformations to extract the dataframe from value field which is a dumped json
    df1 = df.selectExpr("CAST(value as STRING) as json")

    df2 = df1.select(from_json(col("json"), schema=schema).alias("data")).select('data.*')
    df2.printSchema() 
    # Using the timestamp given by kafka as medical transactions are not live
    df3 = df2.withColumn("year", year(df2.created_at)) \
             .withColumn("month", month(df2.created_at)) \
             .withColumn("day", dayofmonth(df2.created_at)) \

    df3.printSchema()
    

    # Writing to hdfs with time partition
    df3.writeStream \
       .outputMode("append") \
       .format("orc") \
       .partitionBy("year", "month", "day") \
       .option("checkpointLocation", checkpoint_directory) \
       .option("path", output_path) \
       .start() \
       .awaitTermination()



if __name__ == "__main__":
    main()




