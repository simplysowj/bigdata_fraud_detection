%spark.pyspark

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import from_json, col
from pyspark.ml.regression import LinearRegression
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import RegressionEvaluator,ClusteringEvaluator
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, OneHotEncoderEstimator, StringIndexer

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

# Convert categorical columns to numerical using StringIndexer
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index", handleInvalid="keep")
                for column in ["Merchant", "TransactionType", "Location"]]

# One-hot encode the indexed categorical columns
encoder = OneHotEncoderEstimator(inputCols=[indexer.getOutputCol() for indexer in indexers],
                                     outputCols=["Merchant_encoded", "TransactionType_encoded", "Location_encoded"])

# Filter out rows with null values in 'Amount'
df_cleaned = df.filter(df["Amount"].isNotNull())

# Assemble features
assembler = VectorAssembler(inputCols=["Amount", "Merchant_encoded","TransactionType_encoded", "Location_encoded"], outputCol="features")
                                
# Define autoencoder model
# autoencoder = LinearRegression(featuresCol="features", labelCol="Amount")
# Define K-Means model
kmeans = KMeans(featuresCol="features", k=3, seed=123)
    
# Create pipeline
pipeline = Pipeline(stages=indexers + [encoder, assembler, kmeans])

# Train the autoencoder model
pipelineModel = pipeline.fit(df_cleaned)

# Evaluate the model
predictions = pipelineModel.transform(df_cleaned)

# Evaluate silhouette score
evaluator = ClusteringEvaluator()
silhouette_score = evaluator.evaluate(predictions)
print("silhouette_score on training data  = %g" % silhouette_score)

#evaluator = RegressionEvaluator(labelCol="Amount", predictionCol="prediction", metricName="rmse")
#rmse = evaluator.evaluate(predictions)
#print("Root Mean Squared Error (RMSE) on training data = %g" % rmse)
        
# Save the trained pipeline model
pipelineModel_path = "/tmp/models/clustering_model"
pipelineModel.write().overwrite().save(pipelineModel_path)

# Stop the SparkSession
#spark.stop()
