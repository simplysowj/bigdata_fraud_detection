%spark.pyspark

from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
import mysql.connector
from pyspark.sql.types import *

# org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.8

spark.sparkContext.setLogLevel("INFO")

# function to insert fraud record to mysql database
def insert_record(row):
    pass
    prediction_value = row["prediction"]
    Amount_value = row["Amount"]
    #print(f" Actual: {Amount_value} Predicted : {prediction_value}")
    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(host="mysql",port=3306,database="FRAUDSDB",user="root",password="abc")
        
        if not connection.is_connected():
            print("failed to connect to sql")
            return
        
        print("inserting record" + row["TransactionID"])
        sql_insert_query = "INSERT INTO fraudtrans VALUES ('" +  row["Timestamp"] + "','" + \
        row["TransactionID"] + "','" + \
        row["AccountID"] + "'," + \
        str(row["Amount"]) + ",'" + \
        row["Merchant"] + "','" + \
        row["TransactionType"] + "','" + \
        row["Location"] + "')"
        
        print(sql_insert_query)
        # Create a cursor object to execute SQL queries
        cursor = connection.cursor()
        cursor.execute(sql_insert_query)
        connection.commit()
        
    except mysql.connector.Error as error:
        print("Error while connecting to MySQL", error)
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Step 1. Create Spark Session
# a SparkSession is a unified entry point for working with structured data. It provides a way to interact with various Spark functionality
appName = "Kafka Examples2"
master = "local"
spark = SparkSession.builder \
    .master(master) \
    .appName(appName) \
    .getOrCreate()

# Step 2. Load the pre-trained model
model_path = "/tmp/models/autoencoder_model"
autoencoder_model = PipelineModel.load(model_path)

# Step 3. Connect and read from kafka topic
kafka_servers = "kafka:9092"
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_servers) \
    .option("subscribe", "my-topic") \
    .option("startingOffsets", "latest") \
    .load() \
df.printSchema()

# Step 4. format the message
json_schema = StructType().add("Timestamp",StringType()) \
    .add("TransactionID",StringType()) \
    .add("AccountID",StringType()) \
    .add("Amount",DecimalType(18,2)) \
    .add("Merchant",StringType()) \
    .add("TransactionType",StringType()) \
    .add("Location",StringType())
    
parsed_df = df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value",json_schema).alias("data")) \
    .select("data.*")
parsed_df.printSchema()

# Step 5. Make predictions on streaming data using the autoencoder model
predictions = autoencoder_model.transform(parsed_df)

# Step 6. Filter anomalies based on threshold
threshold = 50  # Example threshold, adjust as per your model's performance
#anomalies_df = predictions.filter("abs(Amount - prediction) >= threshold")
anomalies_df = predictions.filter("abs(Amount - prediction) >= 0")
anomalies_df.printSchema()

# step 7. write the anomalies to mysql database
query = anomalies_df \
    .writeStream \
    .foreach(insert_record) \
    .option("checkpointLocation", "/tmp/cp/") \
    .start()
    
# step 8. wait for data from kafka topic
query.awaitTermination()
spark.stop()
