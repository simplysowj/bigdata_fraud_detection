%spark.pyspark
from pyspark.sql import SparkSession

appName = "Kafka Examples"
master = "local"

spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .config("org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8") \
    .config("mysql:mysql-connector-java:8.0.11") \
    .getOrCreate()

kafka_servers = "kafka:9092"

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "my-topic") \
    .load()
    
df = df.withColumn('key_str', df['key'].cast('string').alias('key_str')).drop(
    'key').withColumn('value_str', df['value'].cast('string').alias('key_str')).drop('value')

df.show(5)

cols = ['value_str']
df = df.select(*cols)

df = df.selectExpr("value_str as transaction")

df.write \
  .format("jdbc") \
  .mode("append")  \
  .option("driver","com.mysql.cj.jdbc.Driver") \
  .option("url", "jdbc:mysql://mysql:3306/FRAUDSDB") \
  .option("useSSL",False) \
  .option("dbtable", "fraudtrans") \
  .option("user", "root") \
  .option("password", "abc") \
  .save()
  
  
  


  
