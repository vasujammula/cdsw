import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
import re

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
data=df.first()
print(data)
spark.sql("DROP TABLE IF EXISTS department")
spark.sql("""CREATE TABLE IF NOT EXISTS department(
    department_id int ,
    department_name string
    )    
    """)

time.sleep(10)

df=spark.sql("show databases")
df.show()

df=spark.sql("show tables")
df.show()



# Fetch Data from existing table
df=spark.sql("SELECT * FROM department")
df.printSchema()
df.show()

# Create a new DataFrame
new_data = [[101, 'Eng'], [102, 'Bio']]
df_new = spark.createDataFrame(new_data, ['department_id', 'department_name'])
df_new.show()

# Insert into Hive table
df_new.write.insertInto('department', overwrite=False)

# Fetch Data from the table
df=spark.sql("SELECT * FROM department")
df.printSchema()
df.show()


# Check the Database and Tables
df=spark.sql("show databases")
df.show()

df=spark.sql("show tables")
df.show()
