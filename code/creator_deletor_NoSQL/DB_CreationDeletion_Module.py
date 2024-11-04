##########################################################################################################################################
##########################################################################################################################################
# Data Parser Module
##########################################################################################################################################
##########################################################################################################################################

##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################
import pymongo
from pymongo import MongoClient
import os
import json


##########################################################################################################################################
# Function: Connect to the MongoDB instance.
##########################################################################################################################################
def connect_to_mongodb(uri="mongodb://localhost:27017/"):
    """
    Connect to the MongoDB instance.
    
    Args:
        uri (str): MongoDB URI connection string.
        
    Returns:
        MongoClient: MongoDB client instance.
    """
    return MongoClient(uri)

##########################################################################################################################################
# Function: To Create collections in MongoDB based on JSON schema files and enforce the schema for validation.
##########################################################################################################################################    

def create_collections_from_schemas(db, schema_folder_path):
    """
    Create collections in MongoDB based on JSON schema files and enforce schema validation.
    Sets up a time series collection for specific collections, such as 'motionsensor'.
    
    Args:
        db: The MongoDB database instance.
        schema_folder_path (str): Path to the folder containing JSON schema files.
    """
    for schema_file in os.listdir(schema_folder_path):
        if schema_file.endswith('.json'):
            collection_name = schema_file.replace("_Schema_1.json", "").lower()
            with open(os.path.join(schema_folder_path, schema_file), 'r') as file:
                schema = json.load(file)

            if collection_name == "motionsensor":
                # Create a time series collection for the motion sensor data
                db.create_collection(
                    collection_name,
                    timeseries={
                        "timeField": "timestamp",  # This is the field to use as the timestamp for each entry
                        "metaField": "metadata",   # Stores metadata like sensor location and recording ID
                        "granularity": "milliseconds"  # Use "seconds" or "minutes" depending on frequency
                    }
                )
                print(f"Created time series collection: {collection_name}")
            else:
                # For non-time series collections, create a regular collection with validation rules
                validation_rules = {"$jsonSchema": schema}
                if collection_name not in db.list_collection_names():
                    db.create_collection(collection_name, validator=validation_rules)
                    print(f"Created collection with schema validation: {collection_name}")
                else:
                    print(f"Collection already exists: {collection_name}")


##########################################################################################################################################
# Function: To List all databases in the MongoDB instance.
##########################################################################################################################################                
def list_databases(client):
    """
    List all databases in the MongoDB instance.
    
    Args:
        client: The MongoDB client instance.
    """
    print("Databases:")
    for db in client.list_database_names():
        print(f" - {db}")

##########################################################################################################################################
# Function: To List all collections in a given database.
##########################################################################################################################################
def list_collections(db):
    """
    List all collections in a given database.
    
    Args:
        db: The MongoDB database instance.
    """
    print(f"Collections in database {db.name}:")
    for collection in db.list_collection_names():
        print(f" - {collection}")

##########################################################################################################################################
# Function: Delete an entire MongoDB database.
##########################################################################################################################################
def delete_database(client, db_name):
    """
    Delete an entire MongoDB database.
    
    Args:
        client: The MongoDB client instance.
        db_name (str): The name of the database to be deleted.
    """
    if db_name in client.list_database_names():
        client.drop_database(db_name)
        print(f"Database '{db_name}' has been deleted.")
    else:
        print(f"Database '{db_name}' does not exist.")
