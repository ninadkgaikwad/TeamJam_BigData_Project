##########################################################################################################################################
##########################################################################################################################################
# Main Script - Creating and Deleting DB
##########################################################################################################################################
##########################################################################################################################################

# MongoDB URI (adjust as needed)
uri = "mongodb://localhost:27017/"

# Folder where the schema files are located
schema_folder_path = "./schemas"  # Replace with your actual schema path

# Database name
db_name = "SensorDatabase"

# Action to perform: "create" or "delete"
action = "create"  # Change to "delete" to delete the database

# Connect to MongoDB
client = connect_to_mongodb(uri)

if action == "create":
    # Create database instance
    db = client[db_name]
    
    # Step 1: Create the database and collections
    print(f"Creating database '{db_name}' and collections from schemas...")
    create_collections_from_schemas(db, schema_folder_path)
    print("Database and collections created successfully!")
    
    # Step 2: Show all databases
    print("\nListing all databases:")
    list_databases(client)
    
    # Step 3: Show collections of the created database
    print(f"\nListing collections in the database '{db_name}':")
    list_collections(client, db_name)

elif action == "delete":
    # Step 4: Delete the database
    print(f"Deleting database '{db_name}'...")
    delete_database(client, db_name)
    
    # Step 5: Confirm deletion by listing databases again
    print("\nListing all databases after deletion:")
    show_databases(client)
