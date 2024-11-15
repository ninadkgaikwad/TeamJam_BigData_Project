from pymongo import MongoClient
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max, avg, stddev, col

# MongoDB Connection URI
uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
database = client.get_database("SensorDatabase")  # Replace with your database name

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SensorStatistics") \
    .getOrCreate()

def query_numerical_data_from_deprcells(recording_id, sensor_location=None, start_timestamp=None, end_timestamp=None):
    """
    Query function to fetch only numerical data from the DeprCells sensor collection
    based on recording_id, optional sensor_location, and timestamp range.

    Args:
        recording_id (str): The recording ID to match.
        sensor_location (str, optional): Location of the sensor (e.g., "hips", "hand").
        start_timestamp (int, optional): Start of the timestamp range.
        end_timestamp (int, optional): End of the timestamp range.

    Returns:
        pd.DataFrame: DataFrame containing the numerical data (timestamp, dbm, mcc, mns).
    """
    # Reference to the DeprCells collection
    collection = database.get_collection("deprcellsensor")
    
    # Build the base query
    query = {"recording_id": recording_id}
    
    # Add sensor_location to the query if provided
    if sensor_location:
        query["sensor_location"] = sensor_location
    
    # Add timestamp range to the query if both start and end timestamps are provided
    if start_timestamp is not None and end_timestamp is not None:
        query["depr_cells_data.timestamp"] = {"$gte": start_timestamp, "$lte": end_timestamp}
    
    # Execute the query and retrieve only the required fields
    projection = {
        "_id": 0,
        "depr_cells_data.timestamp": 1,
        "depr_cells_data.dbm": 1,
        "depr_cells_data.mcc": 1,
        "depr_cells_data.mns": 1
    }
    
    results = collection.find(query, projection)
    
    # Extract numerical data from the nested array and flatten it into a list of dictionaries
    results_list = []
    for document in results:
        for item in document.get("depr_cells_data", []):
            # Only keep entries that contain all required numerical fields
            if all(key in item for key in ["timestamp", "dbm", "mcc", "mns"]):
                results_list.append({
                    "timestamp": item["timestamp"],
                    "dbm": item["dbm"],
                    "mcc": item["mcc"],
                    "mns": item["mns"]
                })
    
    # Convert results to DataFrame
    df = pd.DataFrame(results_list)
    return df

# Function to calculate statistics using Spark on a given DataFrame
def calculate_statistics_from_dataframe(spark_df):
    """
    Calculate statistics using Spark on the provided DataFrame.

    Args:
        spark_df (DataFrame): Spark DataFrame containing numerical data.

    Returns:
        DataFrame: Spark DataFrame with calculated statistics.
    """
    stats = spark_df.groupBy().agg(
        count("*").alias("frequency"),
        min("timestamp").alias("min_timestamp"),
        max("timestamp").alias("max_timestamp"),
        avg("dbm").alias("avg_dbm"),
        stddev("dbm").alias("stddev_dbm"),
        avg("mcc").alias("avg_mcc"),
        stddev("mcc").alias("stddev_mcc"),
        avg("mns").alias("avg_mns"),
        stddev("mns").alias("stddev_mns")
    )
    return stats

# Example usage
recording_id = "User2-140617"
sensor_location = "hips"
start_timestamp = 1497427158140
end_timestamp = 1497427899496

# Query data from MongoDB for DeprCells numerical fields
numerical_df = query_numerical_data_from_deprcells(recording_id, sensor_location, start_timestamp, end_timestamp)

# Convert Pandas DataFrame to Spark DataFrame and calculate statistics
if not numerical_df.empty:
    spark_df = spark.createDataFrame(numerical_df)
    depr_cells_stats = calculate_statistics_from_dataframe(spark_df)
    depr_cells_stats.show()
else:
    print("No data found for the specified query parameters.")