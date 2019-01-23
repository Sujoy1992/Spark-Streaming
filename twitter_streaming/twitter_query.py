from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Run_Twitter_Query") \
    .getOrCreate()


df = spark.read.option("mergeSchema",True) \
.orc('/tmp/t3/*')

df1 = df.select('location').groupBy('location').count().sort(col('count').desc())
df1.printSchema()
df1.show()
