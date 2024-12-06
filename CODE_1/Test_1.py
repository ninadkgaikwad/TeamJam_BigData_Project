# Connect to MongoDB
from pymongo import MongoClient 

mongo_uri = "mongodb://localhost:27017/"  # Update MongoDB URI if needed
db_name = "SensorDatabase"

# List of available collection names
collections = [
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

client = MongoClient(mongo_uri)

# Specify the database
db = client[db_name]

# Function to delete only the contents of a specified collection
def delete_collection_contents(collection_name):
    if collection_name in collections:
        if collection_name in db.list_collection_names():
            result = db[collection_name].delete_many({})  # Delete all documents in the collection
            print(f"Deleted {result.deleted_count} documents from collection '{collection_name}'.")
        else:
            print(f"Collection '{collection_name}' does not exist in the database '{db_name}'.")
    else:
        print(f"Collection '{collection_name}' is not in the predefined list of collections.")

# Specify the collection whose contents you want to delete
collection_name_to_clear = collections[6]  # Change the index to specify a collection
delete_collection_contents(collection_name_to_clear)
