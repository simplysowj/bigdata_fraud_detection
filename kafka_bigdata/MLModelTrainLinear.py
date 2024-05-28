%spark.pyspark

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import from_json, col
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import *

# Create SparkSession
spark = SparkSession.builder \
    .appName("AutoencoderTraining") \
    .getOrCreate()

columns = ["Timestamp","TransactionID", "AccountID", "Amount", "Merchant", "TransactionType","Location"]
df = spark.read.csv("/data/financial_anomaly_data.csv",header=True, inferSchema=True)

for i, column in enumerate(columns):
    df = df.withColumnRenamed(df.columns[i], column)
# Display schema and first few rows of the DataFrame
df.printSchema()
df.show(5, truncate=False)

# Data Cleaning and Preprocessing
# Assuming 'Timestamp' is in string format, convert it to datetime
df = df.withColumn("Timestamp", col("Timestamp").cast("timestamp"))

# Filter out rows with null values in 'Amount'
df_cleaned = df.filter(df["Amount"].isNotNull())

#df = df.withColumn("Amount",col("Amount").cast(DecimalType(18,2)))

# Assemble features
assembler = VectorAssembler(inputCols=["Amount"], outputCol="features")

# Define autoencoder model
autoencoder = LinearRegression(featuresCol="features", labelCol="Amount")

# Create pipeline
pipeline = Pipeline(stages=[assembler, autoencoder])

# Train the autoencoder model
model = pipeline.fit(df_cleaned)

# Evaluate the model
predictions = model.transform(df_cleaned)
evaluator = RegressionEvaluator(labelCol="Amount", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on training data = %g" % rmse)

# Save the trained model
model_path = "/tmp/models/autoencoder_model"
model.save(model_path)

# Stop the SparkSession
spark.stop()
