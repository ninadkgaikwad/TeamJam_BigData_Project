##########################################################################################################################################
##########################################################################################################################################
# Main Script - DB Ingestion
##########################################################################################################################################
##########################################################################################################################################

##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################
import pymongo
import time
import csv

##########################################################################################################################################
# Import Custom Modules
##########################################################################################################################################
import DataIngestion_Module as helper

##########################################################################################################################################
# Custom Functions
##########################################################################################################################################
def save_to_csv(filename, data):
    """
    Save the average insertion times to a CSV file.

    Args:
        filename (str): The name of the CSV file.
        data (dict): A dictionary of collection names and their average insertion times.
    """
    with open(filename, mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Collection', 'Average Insertion Time (seconds)'])
        for collection, avg_time in data.items():
            writer.writerow([collection, f"{avg_time:.6f}"])


##########################################################################################################################################
# Main Script
##########################################################################################################################################

## User Inputs   
base_path = "/Users/ary_d/OneDrive - Washington State University (email.wsu.edu)/Desktop/userdata/Uncompressed"  # Update this path to your base data directory

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

# Ask the user to select which collections to populate
print("Available collections to work on:")
for idx, collection in enumerate(collections):
    print(f"{idx+1}. {collection}")

selected_collections = input("Enter the numbers of the collections you want to work on (comma-separated, e.g., 1,2,3): ")
selected_indices = [int(x.strip())-1 for x in selected_collections.split(",") if x.strip().isdigit()]

# Dictionary to hold average times for each collection
avg_insertion_times = {}

# Populate data for each selected collection and record the average insertion time
for idx in selected_indices:
    if 0 <= idx < len(collections):
        collection_name = collections[idx]
        avg_time = helper.populate_sensor_collection(db_name, collection_name, base_path, mongo_uri)
        # Store the average time for this collection
        if avg_time is not None:
            avg_insertion_times[collection_name] = avg_time
        else:
            print(f"Skipping collection: {collection_name} due to missing data.")
    else:
        print(f"Invalid selection: {idx+1}")

# Save the average insertion times to a CSV file
if avg_insertion_times:
    csv_filename = "average_insertion_times.csv"
    save_to_csv(csv_filename, avg_insertion_times)
    print(f"\nAverage insertion times have been saved to {csv_filename}.")
else:
    print("\nNo data to save.")



