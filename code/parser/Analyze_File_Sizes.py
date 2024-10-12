import zipfile
import pandas as pd
import os

                            ### SAMPLE OUTPUT ###

#      User ID       Recording ID  ...          File Name File Size (Bytes)
# 0             Unknown Recording  ...                                    0
# 1    LICENSE  Unknown Recording  ...            LICENSE              1535
# 2      User2                     ...                                    0
# 3      User2             140617  ...                                    0
# 4      User2             140617  ...          00inf.txt               130
# ..       ...                ...  ...                ...               ...
# 153    User2             180717  ...     Torso_WiFi.txt          13761261
# 154    User2             180717  ...    videooffset.txt                32
# 155    User2             180717  ...   videospeedup.txt                 3
# 156    User2  datasetstatus.mat  ...  datasetstatus.mat              4380
# 157    User2    labelstatus.mat  ...    labelstatus.mat              2539
#
# [158 rows x 5 columns]
#
# Process finished with exit code 0

def primary():
    # List of zip file paths
    zip_filepaths = [
        '/path/to/SHLDataset_User2.zip',
        '/path/to/SHLDataset_User2.zip',
        '/path/to/SHLDataset_User3.zip'
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
    #file_sizes_df.to_csv('detailed_file_sizes_summary.csv', index=False)

def calculate_total_size_per_user(df):
    """
    Calculates the total file size for each user.

    Args:
        df (pd.DataFrame): The DataFrame containing file sizes and user information.

    Returns:
        pd.DataFrame: A DataFrame containing the total file size per user.
    """
    # Group the data by 'User ID' and sum the 'File Size (Bytes)'
    total_size_df = df.groupby('User ID')['File Size (Bytes)'].sum().reset_index()

    # Rename the column for clarity
    total_size_df.columns = ['User ID', 'Total File Size (Bytes)']

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

    with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
        # Loop through each file in the zip archive
        for file_info in zip_ref.infolist():
            # Extract file name details
            file_path = file_info.filename
            file_size = file_info.file_size

            # Split the path to get detailed information
            path_parts = file_path.split('/')
            user_id = path_parts[1] if len(path_parts) > 1 else 'Unknown User'
            recording_id = path_parts[2] if len(path_parts) > 2 else 'Unknown Recording'
            position = path_parts[3].split('_')[0] if len(path_parts) > 3 and '_' in path_parts[3] else 'Unknown Position'
            file_name = path_parts[-1]

            # Append the information to a list
            file_info_list.append({
                'User ID': user_id,
                'Recording ID': recording_id,
                'Position': position,
                'File Name': file_name,
                'File Size (Bytes)': file_size
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

primary()
