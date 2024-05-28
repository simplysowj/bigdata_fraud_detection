import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Dense

# Load your dataset (assuming it's in a CSV format)
data = pd.read_csv('/content/sample_data/financial_anomaly_data.csv')

data = data.dropna(subset=['Amount'])

# Preprocess the data
# Select relevant features (excluding non-numeric and timestamp columns)
features = data.drop(columns=['Timestamp', 'TransactionID', 'AccountID', 'Merchant', 'TransactionType', 'Location'])

# Standardize the data
scaler = StandardScaler()
scaled_features = scaler.fit_transform(features)

# Train-test split
X_train, X_test = train_test_split(scaled_features, test_size=0.2, random_state=42)

# Define the autoencoder architecture
input_dim = X_train.shape[1]
encoding_dim = 32  # Choose the size of the encoding layer as per your requirement

input_layer = Input(shape=(input_dim,))
encoder = Dense(encoding_dim, activation='relu')(input_layer)
decoder = Dense(input_dim, activation='relu')(encoder)

# Define the autoencoder model
autoencoder = Model(inputs=input_layer, outputs=decoder)

# Compile the model
autoencoder.compile(optimizer='adam', loss='mse')

# Train the autoencoder
autoencoder.fit(X_train, X_train, epochs=50, batch_size=128, shuffle=True, validation_data=(X_test, X_test))

# Get the encoded representations
encoded_features = autoencoder.predict(scaled_features)

# Apply k-means clustering on the encoded representations
kmeans = KMeans(n_clusters=2, random_state=42)
kmeans.fit(encoded_features)

# Evaluate clustering performance using silhouette score
silhouette_avg = silhouette_score(encoded_features, kmeans.labels_)
print(f"Silhouette Score: {silhouette_avg}")

# Identify anomalies based on clustering
cluster_centers = kmeans.cluster_centers_
distances = np.linalg.norm(encoded_features - cluster_centers[kmeans.labels_], axis=1)
threshold = np.percentile(distances, 95)

# Find anomalies
anomalies = data[distances > threshold]
print("Anomalies:")
print(anomalies)
