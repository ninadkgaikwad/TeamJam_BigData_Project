##########################################################################################################################################
##########################################################################################################################################
# Main Script - Creating and Deleting DB
##########################################################################################################################################
##########################################################################################################################################
import DB_CreationDeletion_Module as file
# MongoDB URI (adjust as needed)
uri = "mongodb://localhost:27017/"

# Folder where the schema files are located
schema_folder_path = "/Users/ary_d/OneDrive - Washington State University (email.wsu.edu)/Desktop/termProj/it2/TeamJam_BigData_Project/json_schemas/ModularizedSchemas"  # Replace with your actual schema path

# Database name
db_name = "SensorDatabase"

# Action to perform: "create" or "delete"
action = "create"  # Change to "delete" to delete the database

# Connect to MongoDB
client = file.connect_to_mongodb(uri)

if action == "create":
    # Create database instance
    db = client[db_name]
    
    # Step 1: Create the database and collections
    print(f"Creating database '{db_name}' and collections from schemas...")
    file.create_collections_from_schemas(db, schema_folder_path)
    print("Database and collections created successfully!")
    
    # Step 2: Show all databases
    print("\nListing all databases:")
    file.list_databases(client)
    
    # Step 3: Show collections of the created database
    print(f"\nListing collections in the database '{db_name}':")
    file.list_collections(client, db_name)

elif action == "delete":
    # Step 4: Delete the database
    print(f"Deleting database '{db_name}'...")
    file.delete_database(client, db_name)
    
    # Step 5: Confirm deletion by listing databases again
    print("\nListing all databases after deletion:")
    file.show_databases(client)
