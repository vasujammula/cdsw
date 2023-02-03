import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
master="local"

spark = SparkSession\
    .builder\
    .appName("HiveAccessCheck")\
    .master(master)\
    .enableHiveSupport() \
    .getOrCreate()

spark

df=spark.sql("show databases")
df.show()

df=spark.sql("show tables")
df.show()
