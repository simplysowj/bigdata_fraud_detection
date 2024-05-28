%spark.pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType

# org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8
# mysql:mysql-connector-java:8.0.11

spark.sparkContext.setLogLevel("INFO")

appName = "Kafka Examples"
master = "local"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Perform a SQL query on the temp table
query_result = spark.sql("SELECT * FROM temp_table")

# Show the query result
query_result.show()

query_result.write \
    .format("jdbc") \
    .mode("append")  \
    .option("driver","com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://mysql:3306/FRAUDSDB") \
    .option("useSSL",False) \
    .option("dbtable", "fraudtrans") \
    .option("user", "root") \
    .option("password", "abc") \
    .save()
  
# Stop the SparkSession
spark.stop()
