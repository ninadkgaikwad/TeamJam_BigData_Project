import zipfile
import pandas as pd
import os

def primary():
    # List of zip file paths
    zip_filepaths = [
        '/path/to/SHLDataset_User2.zip'
    ]

    file_sizes_df = analyze_multiple_zip_files_with_structure(zip_filepaths)

    # Check if DataFrame is empty
    if file_sizes_df.empty:
        print("No data found in the zip file(s).")
    else:
        print("Data found:")
        print(file_sizes_df)

    # Calculate total size per user
    total_sizes_df = calculate_total_size_per_user(file_sizes_df)
    print("\nTotal Data Size per User:")
    print(total_sizes_df)

    # Optionally, save the DataFrame to a CSV file for further analysis
    # file_sizes_df.to_csv('detailed_file_sizes_summary.csv', index=False)

def calculate_total_size_per_user(df):
    """
    Calculates the total file size for each user.

    Args:
        df (pd.DataFrame): The DataFrame containing file sizes and user information.

    Returns:
        pd.DataFrame: A DataFrame containing the total file size per user in megabytes.
    """
    # Group the data by 'User ID' and sum the 'File Size (MB)'
    total_size_df = df.groupby('User ID')['File Size (MB)'].sum().reset_index()

    # Rename the column for clarity
    total_size_df.columns = ['User ID', 'Total File Size (MB)']

    return total_size_df

def analyze_zip_file_sizes_by_structure(zip_filepath):
    """
    Analyzes the sizes of files inside a zip file considering the detailed file structure and returns a DataFrame.

    Args:
        zip_filepath (str): The path to the zip file.

    Returns:
        pd.DataFrame: A DataFrame containing the file sizes categorized by user, recording ID, and phone position.
    """
    file_info_list = []

    # Define file extensions and specific files to ignore
    ignored_extensions = ['.mat', '.kml', '.AVI']
    ignored_filenames = ['videooffset.txt', 'videospeedup.txt']

    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        # Loop through each file in the zip archive
        for file_info in zip_ref.infolist():
            # Extract file name details
            file_path = file_info.filename
            file_size = file_info.file_size

            # Get the base name and extension of the file
            file_name, file_extension = os.path.splitext(file_path)

            # Check if the file should be ignored
            if file_extension in ignored_extensions or os.path.basename(file_path) in ignored_filenames:
                continue  # Skip ignored files

            # Remove the file extension for cleaner output
            clean_file_name = os.path.basename(file_name)

            # Split the path to get detailed information
            path_parts = file_path.split('/')
            user_id = path_parts[1] if len(path_parts) > 1 else 'Unknown User'
            recording_id = path_parts[2] if len(path_parts) > 2 else 'Unknown Recording'
            position = path_parts[3].split('_')[0] if len(path_parts) > 3 and '_' in path_parts[3] else 'Unknown Position'

            # Convert file size from bytes to megabytes
            file_size_mb = file_size / (1024 * 1024)

            # Append the information to a list
            file_info_list.append({
                'User ID': user_id,
                'Recording ID': recording_id,
                'Position': position,
                'File Name': clean_file_name,  # File name without extension
                'File Size (MB)': round(file_size_mb, 2)  # Size in MB
            })

    # Convert the list of file information into a Pandas DataFrame
    df = pd.DataFrame(file_info_list)
    return df

def analyze_multiple_zip_files_with_structure(zip_filepaths):
    """
    Analyzes file sizes from multiple zip files considering the detailed file structure and returns a combined DataFrame.

    Args:
        zip_filepaths (list): List of paths to the zip files.

    Returns:
        pd.DataFrame: A DataFrame containing file sizes for all users, recordings, and positions.
    """
    all_data = pd.DataFrame()

    for zip_filepath in zip_filepaths:
        df = analyze_zip_file_sizes_by_structure(zip_filepath)
        all_data = pd.concat([all_data, df], ignore_index=True)

    return all_data

# Run the primary function
primary()
