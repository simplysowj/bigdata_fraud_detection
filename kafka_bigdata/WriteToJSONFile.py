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

kafka_servers = "kafka:9092"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "earliest") \
    .load() \

df.printSchema()

json_schema = StructType().add("transaction",StringType())
parsed_df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value",json_schema).alias("data")) \
    .select("data.*")

parsed_df.printSchema()
        
query = parsed_df \
    .writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "/tmp/data/") \
    .option("checkpointLocation", "/tmp/cp/") \
    .start()
            
query.awaitTermination()
spark.stop()
