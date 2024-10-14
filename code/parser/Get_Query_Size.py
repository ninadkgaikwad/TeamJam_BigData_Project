import pymongo
import pandas as pd
import bson  # For calculating document size
from Analyze_File_Sizes import get_df_raw  # Assuming this is the first file

def get_mongodb_data_and_size(db_name, collection_names, mongo_uri="mongodb://localhost:27017/"):
    """
    Retrieve MongoDB documents, calculate their size, group by recording_id, and store the aggregated information in a pandas dataframe.
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
            recording_id = document.get("recording_id", "Unknown Recording")  # Handle missing recording_id

            # Calculate the size of the document in bytes using BSON encoding
            document_size_bytes = len(bson.BSON.encode(document))
            document_size_mb = document_size_bytes / (1024 * 1024)  # Convert to MB

            # Append the data to the list
            data.append({
                "recording_id": recording_id,
                "file_size_mb": document_size_mb
            })

    # Convert the list of dictionaries to a pandas dataframe
    df_mongodb = pd.DataFrame(data)

    # Group by recording_id and sum file sizes
    df_mongodb_grouped = df_mongodb.groupby('recording_id')['file_size_mb'].sum().reset_index()

    # Now, group by user (User1, User2, etc.) based on the recording_id prefix
    df_mongodb_grouped['user'] = df_mongodb_grouped['recording_id'].apply(lambda x: x.split('-')[0] if '-' in x else "Unknown User")

    # Group by user and sum all recording sizes
    df_user_total_sizes = df_mongodb_grouped.groupby('user')['file_size_mb'].sum().reset_index()

    # Rename columns for clarity
    df_user_total_sizes.columns = ['User', 'Total File Size (MB)']

    return df_user_total_sizes


def compare_raw_and_mongo_data(folder_paths, db_name, collection_names):
    """
    Compares file sizes in raw data with sizes in MongoDB.
    """
    # Get df_raw by calling the function from Analyze_File_Sizes.py
    df_raw = get_df_raw(folder_paths)

    if df_raw is None:
        print("No raw data found.")
        return

    print("df_raw columns:", df_raw.columns)  # Check df_raw columns
    df_raw.rename(columns={'Recording ID': 'recording_id'}, inplace=True)  # Rename to match

    # Get MongoDB data
    df_mongodb = get_mongodb_data_and_size(db_name, collection_names)

    print("df_mongodb columns:", df_mongodb.columns)  # Check df_mongodb columns

    # Merge the two DataFrames for comparison
    df_comparison = pd.merge(df_raw, df_mongodb, left_on='recording_id', right_on='User', how='outer', suffixes=('_raw', '_mongo'))

    # Print DataFrames
    print("\nRaw DataFrame:")
    print(df_raw)

    print("\nMongoDB DataFrame:")
    print(df_mongodb)

    print("\nComparison DataFrame:")
    print(df_comparison)

    # Write to CSV
    df_raw.to_csv("raw_file_sizes.csv", index=False)
    df_mongodb.to_csv("mongodb_file_sizes.csv", index=False)
    df_comparison.to_csv("comparison_file_sizes.csv", index=False)

    return df_comparison


def get_collection_size(db_name, collection_name, mongo_uri="mongodb://localhost:27017/"):
    """
    Calculate the total size of a MongoDB collection in MB.
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
    """
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]
    stats = db.command("dbStats")
    db_size_bytes = stats["storageSize"]  # Size in bytes
    db_size_mb = db_size_bytes / (1024 * 1024)  # Convert to MB
    return db_size_mb


def compare_total_sizes(df_raw, df_mongodb):
    """
    Compare the total size of files for each recording from the raw data and MongoDB data.
    """
    # Group by recording_id and calculate total file size for each recording in both raw and MongoDB data
    total_raw_sizes = df_raw.groupby('recording_id')['file_size_mb'].sum().reset_index()
    total_raw_sizes.rename(columns={'file_size_mb': 'total_file_size_mb_raw'}, inplace=True)

    total_mongodb_sizes = df_mongodb.groupby('recording_id')['file_size_mb'].sum().reset_index()
    total_mongodb_sizes.rename(columns={'file_size_mb': 'total_file_size_mb_mongodb'}, inplace=True)

    # Merge the two dataframes on recording_id
    df_comparison = pd.merge(total_raw_sizes, total_mongodb_sizes, on='recording_id', how='outer')

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

folder_paths = [
    r'C:\Users\ary_d\OneDrive - Washington State University (email.wsu.edu)\Desktop\userdata\Uncompressed\User1',
    r'C:\Users\ary_d\OneDrive - Washington State University (email.wsu.edu)\Desktop\userdata\Uncompressed\User2',
    r'C:\Users\ary_d\OneDrive - Washington State University (email.wsu.edu)\Desktop\userdata\Uncompressed\User3'
]  # Add your folder paths here

# Compare raw and MongoDB data
df_comparison = compare_raw_and_mongo_data(folder_paths, "SensorDatabase", collection_names)
print(df_comparison)

# Get collection and database sizes
for collection in collection_names:
    print(f"Size of {collection} collection: {get_collection_size('SensorDatabase', collection)} MB")

print(f"Total size of SensorDatabase: {get_database_size('SensorDatabase')} MB")
