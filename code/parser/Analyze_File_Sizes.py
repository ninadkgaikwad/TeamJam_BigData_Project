import os
import pandas as pd

def primary():
    # List of directory paths (instead of zip files)
    folder_paths = [
        r'/Users/ary_d/OneDrive - Washington State University (email.wsu.edu)/Desktop/userdata/Uncompressed',
        r'/Users/ary_d/OneDrive - Washington State University (email.wsu.edu)/Desktop/userdata/Uncompressed',
        r'/Users/ary_d/OneDrive - Washington State University (email.wsu.edu)/Desktop/userdata/Uncompressed'
    ]

    file_sizes_df = analyze_multiple_directories(folder_paths)

    # Check if DataFrame is empty
    if file_sizes_df.empty:
        print("No data found in the folder(s).")
    else:
        print("Data found:")
        print(file_sizes_df)

    # Calculate total size per recording
    total_sizes_df = calculate_total_size_per_recording(file_sizes_df)
    print("\nTotal Data Size per Recording:")
    print(total_sizes_df)

    # Write to CSV
    file_sizes_df.to_csv('raw_file_sizes.csv', index=False)
    total_sizes_df.to_csv('total_file_sizes_per_recording.csv', index=False)

    return total_sizes_df

def calculate_total_size_per_recording(df):
    """
    Calculates the total file size for each recording.
    """
    # Group the data by 'Recording ID' and sum the 'File Size (MB)'
    total_size_df = df.groupby('Recording ID')['File Size (MB)'].sum().reset_index()

    # Rename the column for clarity
    total_size_df.columns = ['Recording ID', 'Total File Size (MB)']

    return total_size_df

def analyze_directory_sizes_by_structure(directory_path):
    """
    Analyzes the sizes of files inside a directory considering the detailed file structure and returns a DataFrame.
    """
    file_info_list = []

    # Define file extensions and specific files to ignore
    ignored_extensions = ['.mat', '.kml', '.AVI']
    ignored_filenames = ['videooffset.txt', 'videospeedup.txt']

    # Walk through the directory structure
    for root, dirs, files in os.walk(directory_path):
        for file_name in files:
            file_path = os.path.join(root, file_name)
            file_size = os.path.getsize(file_path)  # File size in bytes

            _, file_extension = os.path.splitext(file_name)
            if file_extension in ignored_extensions or file_name in ignored_filenames:
                continue  # Skip ignored files

            path_parts = root.split(os.sep)
            if len(path_parts) < 4:
                continue  # Skip if the folder structure doesn't have enough depth

            recording_id = path_parts[-2]  # 'Recording1', 'Recording2', etc.
            position = path_parts[-1] if len(path_parts) > 3 else 'Unknown Position'

            file_size_mb = file_size / (1024 * 1024)  # Convert file size to MB

            file_info_list.append({
                'Recording ID': recording_id,
                'Position': position,
                'File Name': file_name,  # File name without extension
                'File Size (MB)': round(file_size_mb, 2)
            })

    # Convert the list of file information into a DataFrame
    df = pd.DataFrame(file_info_list)
    return df

def analyze_multiple_directories(directory_paths):
    """
    Analyzes file sizes from multiple directories considering the detailed file structure.
    """
    all_data = pd.DataFrame()

    for directory_path in directory_paths:
        df = analyze_directory_sizes_by_structure(directory_path)
        all_data = pd.concat([all_data, df], ignore_index=True)

    return all_data

def get_df_raw(folder_paths):
    """
    Function to run the analysis and return the raw DataFrame (df_raw).
    """
    file_sizes_df = analyze_multiple_directories(folder_paths)

    if file_sizes_df.empty:
        print("No data found in the folder(s).")
        return None

    total_sizes_df = calculate_total_size_per_recording(file_sizes_df)
    return total_sizes_df
