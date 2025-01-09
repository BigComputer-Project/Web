import os
import uuid
import pandas as pd
from pathlib import Path
import argparse

def process_parquet_files(folder_path):
    """
    Loads all Parquet files under a folder, adds a column 'id' with a unique UUID, and saves them back.

    :param folder_path: Path to the folder containing Parquet files.
    """
    folder = Path(folder_path)
    if not folder.is_dir():
        raise ValueError(f"Provided path '{folder_path}' is not a valid directory.")

    # Loop through all Parquet files in the folder
    for file_path in folder.glob("*.parquet"):
        try:
            # Load the Parquet file into a DataFrame
            df = pd.read_parquet(file_path)

            # Add a new 'id' column with UUIDs
            df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]

            # Save the updated DataFrame back to the same file
            df.to_parquet(file_path, index=False)
            print(f"Processed and updated: {file_path}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Process Parquet files by adding a UUID column.")
    parser.add_argument("folder_path", type=str, help="Path to the folder containing Parquet files.")
    args = parser.parse_args()

    process_parquet_files(args.folder_path)

if __name__ == "__main__":
    main()
