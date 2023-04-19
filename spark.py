import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.ensemble import IsolationForest
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.functions import col, lag


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Basic Spark Test") \
    .getOrCreate()

# Define the directory containing the CSV files
data_directory = "output_data"

# Loop through all CSV files in the directory and union them to create a single DataFrame
data = None
for file_name in os.listdir(data_directory):
    if file_name.endswith(".csv"):
        csv_file = os.path.join(data_directory, file_name)
        temp_data = spark.read.csv(csv_file, header=True, inferSchema=True)
        if data is None:
            data = temp_data
        else:
            data = data.union(temp_data)

if data:
    # Show the schema and a few rows
    data.printSchema()
    data.show(3)

    # Calculate the time difference between consecutive records for each driver
    data = data.withColumn(
        "time_difference",
        (col("Time").cast("long") - lag("Time").over(Window.partitionBy("driverID").orderBy("Time")).cast("long")) / 3600,
    )

    # Calculate the distance traveled in each record
    data = data.withColumn("distance", col("Speed") * col("time_difference"))

    # Perform a basic aggregation operation
    aggregated_data = (
        data.groupBy("driverID")
        .agg(
            F.sum("isRapidlySpeedup").alias("total_rapid_accelerations"),
            F.sum("isRapidlySlowdown").alias("total_rapid_decelerations"),
            F.sum("isNeutralSlide").alias("total_neutral_slide"),
            F.sum("neutralSlideTime").alias("total_neutral_slide_duration"),
            F.sum("isOverspeed").alias("total_overspeed"),
            F.sum("overspeedTime").alias("total_overspeed_duration"),
            F.sum("isFatigueDriving").alias("total_fatigue_driving"),
            F.sum("isHthrottleStop").alias("total_hthrottle_stop"),
            F.sum("isOilLeak").alias("total_oil_leak"),
            F.avg("Speed").alias("average_speed"),
            F.sum("distance").alias("total_distance"),
        )
    )

    aggregated_data.show()

    # Convert the Spark DataFrame to a pandas DataFrame
    data = aggregated_data.toPandas()

    # Stop the Spark session
    spark.stop()

else:
    print("No CSV files found in the specified directory.")


# Statistical analysis
mean_values = data.mean(numeric_only=True)
median_values = data.median(numeric_only=True)
std_values = data.std(numeric_only=True)
correlation_matrix = data.corr(method='pearson', numeric_only=True)

# Data visualization
# Box plot for total_rapid_accelerations
plt.figure()
sns.boxplot(x='total_rapid_accelerations', data=data)
plt.show()

# Bar chart for total_overspeed_duration
plt.figure()
sns.barplot(x='driverID', y='total_overspeed_duration', data=data)
plt.xticks(rotation=90)
plt.show()

# Correlation analysis
correlation_matrix = data.corr(method='pearson', numeric_only=True)
print("Correlation matrix:")
print(correlation_matrix)

# Ranking and clustering
features = data[['total_rapid_accelerations', 'total_rapid_decelerations','total_neutral_slide','total_neutral_slide_duration','total_overspeed','total_overspeed_duration','total_fatigue_driving','total_hthrottle_stop','total_oil_leak','average_speed','total_distance']]
features.columns = [['total_rapid_accelerations', 'total_rapid_decelerations','total_neutral_slide','total_neutral_slide_duration','total_overspeed','total_overspeed_duration','total_fatigue_driving','total_hthrottle_stop','total_oil_leak','average_speed','total_distance']]
kmeans = KMeans(n_clusters=3, n_init=10, random_state=0)
data['cluster'] = kmeans.fit_predict(features)
print('')
print("Cluster assignments:")
print(data['cluster'].value_counts())

# Regression analysis
X = data[['total_neutral_slide','total_neutral_slide_duration','total_overspeed','total_overspeed_duration','total_fatigue_driving','total_hthrottle_stop','total_oil_leak']]
y = data['average_speed']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)

regression = LinearRegression()
regression.fit(X_train, y_train)

y_pred = regression.predict(X_test)
print('')
print("Regression coefficients:")
print(regression.coef_)
print('')
print("Regression score (R^2):")
print(regression.score(X_test, y_test))

# Anomaly detection
iso_forest = IsolationForest(contamination=0.1 )

iso_forest.fit(features)

data['anomaly'] = iso_forest.predict(features)
print('')
print("Anomaly counts:")
print(data['anomaly'].value_counts())