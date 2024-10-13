import pymongo
import pandas as pd
import bson  # For calculating document size
from Analyze_File_Sizes import get_df_raw


def get_mongodb_data_and_size(db_name, collection_names, mongo_uri="mongodb://localhost:27017/"):
    """
    Retrieve MongoDB documents, calculate their size, and store the information in a pandas dataframe.

    Args:
        db_name (str): Name of the MongoDB database.
        collection_names (list): List of collection names to retrieve data from.
        mongo_uri (str): MongoDB URI to connect to the database (default is "mongodb://localhost:27017/").

    Returns:
        pd.DataFrame: DataFrame containing user_id, recording_id, and document size in MB.
    """
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]

    data = []

    # Loop through each collection
    for collection_name in collection_names:
        collection = db[collection_name]

        # Retrieve all documents in the collection
        documents = collection.find()

        # Loop through each document and calculate its size
        for document in documents:
            user_id = document["_id"].split("-")[0]  # Extract user_id from _id
            recording_id = document["recording_id"]  # Extract recording_id

            # Calculate the size of the document in bytes using BSON encoding
            document_size_bytes = len(bson.BSON.encode(document))
            document_size_mb = document_size_bytes / (1024 * 1024)  # Convert to MB

            # Append the data to the list
            data.append({
                "user_id": user_id,
                "recording_id": recording_id,
                "file_size_mb": document_size_mb
            })

    # Convert the list of dictionaries to a pandas dataframe
    df_mongodb = pd.DataFrame(data)

    return df_mongodb

def compare_raw_and_mongo_data(zip_filepaths, db_name, collection_names):
    """
    Compares file sizes in raw data with sizes in MongoDB.
    """
    # Get df_raw by calling the function from Analyze_File_Sizes.py
    df_raw = get_df_raw(zip_filepaths)

    if df_raw is None:
        print("No raw data found.")
        return

    # Get MongoDB data
    df_mongodb = get_mongodb_data_and_size(db_name, collection_names)

    # Merge the two DataFrames for comparison
    df_comparison = pd.merge(df_raw, df_mongodb, on=['User ID', 'Recording ID'], how='outer', suffixes=('_raw', '_mongo'))

    return df_comparison

def get_collection_size(db_name, collection_name, mongo_uri="mongodb://localhost:27017/"):
    """
    Calculate the total size of a MongoDB collection in MB.

    Args:
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the collection to get the size for.
        mongo_uri (str): MongoDB URI to connect to the database (default is "mongodb://localhost:27017/").

    Returns:
        float: The size of the collection in MB.
    """
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    stats = db.command("collStats", collection_name)
    collection_size_bytes = stats["storageSize"]  # Size in bytes
    collection_size_mb = collection_size_bytes / (1024 * 1024)  # Convert to MB
    return collection_size_mb


def get_database_size(db_name, mongo_uri="mongodb://localhost:27017/"):
    """
    Calculate the total size of a MongoDB database in MB.

    Args:
        db_name (str): Name of the MongoDB database.
        mongo_uri (str): MongoDB URI to connect to the database (default is "mongodb://localhost:27017/").

    Returns:
        float: The size of the database in MB.
    """
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    stats = db.command("dbStats")
    db_size_bytes = stats["storageSize"]  # Size in bytes
    db_size_mb = db_size_bytes / (1024 * 1024)  # Convert to MB
    return db_size_mb


def compare_total_sizes(df_raw, df_mongodb):
    """
    Compare the total size of files for each user from the raw data and MongoDB data.

    Args:
        df_raw (pd.DataFrame): DataFrame containing raw data with 'user_id', 'recording_id', and 'file_size_mb_raw'.
        df_mongodb (pd.DataFrame): DataFrame containing MongoDB data with 'user_id', 'recording_id', and 'file_size_mb_mongodb'.

    Returns:
        pd.DataFrame: DataFrame comparing the total file sizes for each user.
    """
    # Group by user_id and calculate total file size for each user in both raw and MongoDB data
    total_raw_sizes = df_raw.groupby('user_id')['file_size_mb'].sum().reset_index()
    total_raw_sizes.rename(columns={'file_size_mb': 'total_file_size_mb_raw'}, inplace=True)

    total_mongodb_sizes = df_mongodb.groupby('user_id')['file_size_mb'].sum().reset_index()
    total_mongodb_sizes.rename(columns={'file_size_mb': 'total_file_size_mb_mongodb'}, inplace=True)

    # Merge the two dataframes on user_id
    df_comparison = pd.merge(total_raw_sizes, total_mongodb_sizes, on='user_id', how='outer')

    # Calculate the difference in sizes
    df_comparison['size_difference_mb'] = df_comparison['total_file_size_mb_raw'] - df_comparison['total_file_size_mb_mongodb']

    return df_comparison


# Example usage:
collection_names = [
    "userdata",
    "recordingdata",
    "ambientsensor",
    "batterysensor",
    "apisensor",
    "locationsensor",
    "motionsensor",
    "deprcellssensor",
    "wifisensor",
    "gpssensor",
    "cellssensor",
    "labelsensor"
]

# Get MongoDB data
df_mongodb = get_mongodb_data_and_size("SensorDatabase", collection_names)

# Assuming df_raw is your raw data DataFrame, with 'user_id', 'recording_id', and 'file_size_mb'
# For example, you may have run something like this earlier for the raw data.
df_raw = pd.DataFrame([
    {"user_id": "User1", "recording_id": "rec001", "file_size_mb": 10.5},
    {"user_id": "User1", "recording_id": "rec002", "file_size_mb": 15.2},
    {"user_id": "User2", "recording_id": "rec003", "file_size_mb": 8.0},
    {"user_id": "User3", "recording_id": "rec004", "file_size_mb": 7.5}
])

# Compare sizes for each user
df_comparison = compare_total_sizes(df_raw, df_mongodb)
print(df_comparison)

# Get collection and database sizes
for collection in collection_names:
    print(f"Size of {collection} collection: {get_collection_size('SensorDatabase', collection)} MB")

print(f"Total size of SensorDatabase: {get_database_size('SensorDatabase')} MB")
