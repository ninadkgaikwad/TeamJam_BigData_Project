import json
from pymongo import MongoClient


def get_query_size_in_mb(query, collection_name, db_name='AA'):
    """
    Executes the query in MongoDB, retrieves the result, and calculates its size in megabytes.

    Args:
        query (dict): The MongoDB query to execute.
        collection_name (str): The name of the collection to query.
        db_name (str): The name of the MongoDB database. Default is 'AA'.

    Returns:
        float: The size of the query result in megabytes (MB).
    """
    # MongoDB connection
    client = MongoClient('mongodb://localhost:27017/')  # Adjust as needed
    db = client[db_name]
    collection = db[collection_name]

    # Execute the query and retrieve results
    results = list(collection.find(query))

    # Convert the results to JSON and calculate the size
    results_json = json.dumps(results)
    size_in_bytes = len(results_json.encode('utf-8'))  # Size in bytes
    size_in_mb = size_in_bytes / (1024 * 1024)  # Convert bytes to MB

    # Return the size in MB
    return round(size_in_mb, 4)


# Example usage
user_id = "12345"  # Replace with the actual user ID

# Call the function to get the size of the query result
query_size_mb = get_query_size_in_mb(query, "ambientsensor")
print(f"Query size: {query_size_mb} MB")
