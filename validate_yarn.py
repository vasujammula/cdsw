# # Estimating $\pi$
#
# This is the simplest PySpark example. It shows how to estimate $\pi$ in parallel
# using Monte Carlo integration. If you're new to PySpark, start here!

from __future__ import print_function
import sys
from random import random
from operator import add
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("PythonPi")\
    .getOrCreate()

result = "yarn" in spark.conf.get("spark.master")

partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
n = 100000 * partitions

def f(_):
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 < 1 else 0

count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
print("Pi is roughly %f" % (4.0 * count / n))

spark.stop()

if result:
    print("Test Case Executed on Pushdown")
    exit(0)
else:
    print("Test Case Failed")
    raise RuntimeError("Failed ..")
