%spark.pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import mysql.connector

# org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8
# mysql:mysql-connector-java:8.0.11

spark.sparkContext.setLogLevel("INFO")

# function to insert fraud record to mysql database
def insert_record(row):
    pass
    #transaction_value = row["transaction"]
    #print(f"Transaction value: {transaction_value}")
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(host="mysql",port=3306,database="FRAUDSDB",user="root",password="abc")
        
        if not connection.is_connected():
            print("failed to connect to sql")
            return
        
        print("inserting record" + row["TransactionID"])
        sql_insert_query = "INSERT INTO fraudtrans VALUES ('" +  row["Timestamp"] + "','" + \
        row["TransactionID"] + "','" + \
        row["AccountID"] + "','" + \
        row["Amount"] + "','" + \
        row["Merchant"] + "','" + \
        row["TransactionType"] + "','" + \
        row["Location"] + "')"
        
        print("inserted record" + row["TransactionID"])
        # Create a cursor object to execute SQL queries
        cursor = connection.cursor()
        cursor.execute(sql_insert_query)
        connection.commit()
        
    except mysql.connector.Error as error:
        print("Error while connecting to MySQL", error)
    finally:
        # Close the cursor and connection
        if 'connection' in locals() and connection.is_connected():
            #cursor.close()
            connection.close()
            print("MySQL connection is closed")

# Step 1. Create Spark Session
# a SparkSession is a unified entry point for working with structured data. It provides a way to interact with various Spark functionality
appName = "Kafka Examples"
master = "local"
spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Step 2. Connect and read from kafka topic
kafka_servers = "kafka:9092"
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "earliest") \
    .load() \

df.printSchema()

# Step 3. format the message
json_schema = StructType().add("Timestamp",StringType()) \
    .add("TransactionID",StringType()) \
    .add("AccountID",StringType()) \
    .add("Amount",StringType()) \
    .add("Merchant",StringType()) \
    .add("TransactionType",StringType()) \
    .add("Location",StringType())
    
parsed_df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value",json_schema).alias("data")) \
    .select("data.*")
parsed_df.printSchema()

# step 4. write the message to mysql
query = parsed_df \
    .writeStream \
    .foreach(insert_record) \
    .start()

# step 5. wait for data from kafka topic
query.awaitTermination()
spark.stop()
