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

# Sample data, replace this with your actual data
data = [("01-01-2023 08:00","TXN1127", "ACC4", 95071.92, "MerchantH", "Purchase", "Tokyo"),
        ("01-01-2023 08:01","TXN1639","ACC10", 15607.89, "MerchantH", "Purchase", "London"),
        ("01-01-2023 08:02","TXN1127","ACC8", 65092.34, "MerchantE", "Withdrawal", "London"),
        ("01-01-2023 08:03","TXN872","ACC6", 87.87, "MerchantE", "Purchase", "London"),
        ("01-01-2023 08:04","TXN1338","ACC4", 716.56, "MerchantI", "Purchase", "Los Angeles")]

schema = ["Timestamp","TransactionID", "AccountID", "Amount", "Merchant", "TransactionType","Location"]
df = spark.createDataFrame(data, schema)

df = df.withColumn("Amount",col("Amount").cast(DecimalType(18,2)))

# Assemble features
assembler = VectorAssembler(inputCols=["Amount"], outputCol="features")

# Define autoencoder model
autoencoder = LinearRegression(featuresCol="features", labelCol="Amount")

# Create pipeline
pipeline = Pipeline(stages=[assembler, autoencoder])

# Train the autoencoder model
model = pipeline.fit(df)

# Evaluate the model
predictions = model.transform(df)
evaluator = RegressionEvaluator(labelCol="Amount", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on training data = %g" % rmse)

# Save the trained model
model_path = "/tmp/models/autoencoder_model"
model.save(model_path)

# Stop the SparkSession
spark.stop()
