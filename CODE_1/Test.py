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

# Specify the database and collection name
collection_name = collections[6]
db = client[db_name]
collection = db[collection_name]

# Retrieve and display documents
documents = collection.find()  # Retrieve all documents in the collection
counter = 0
for doc in documents:
    counter= counter+1
    print(doc)
    if (counter>1):
        break
