# python extract_url.py "/mnt/hdd/benjamin/fineweb/350BT/sample/350BT" "/home/benjamin/ssd/kaixin/MMStack-Web/urls/350BT"
import os
import pandas as pd
import uuid
import argparse
from tqdm import tqdm
from multiprocessing import Pool

def process_file(file_path, output_folder, file_number):
    """Process a single Parquet file to extract URLs and generate UUIDs, then save it in the current process with a numeric name."""
    try:
        df = pd.read_parquet(file_path)

        # Check if 'url' column exists
        if 'url' in df.columns:
            sub_df = df[['url']].copy()
            del df
            # Generate UUID for each URL entry
            sub_df['uuid'] = sub_df.apply(lambda x: str(uuid.uuid4()), axis=1)
            
            # Save the processed data with a numeric file name
            output_file = os.path.join(output_folder, f'{file_number}.parquet')
            sub_df.to_parquet(output_file, index=False)
        else:
            tqdm.write(f"Skipping file {file_path}: 'url' column not found.")
    except Exception as e:
        tqdm.write(f"Error processing file {file_path}: {e}")

def process_parquet_files(input_folder, output_folder, num_processes=16):
    # List all Parquet files in the input folder
    parquet_files = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith('.parquet')]

    # Use multiprocessing to process files concurrently
    with Pool(processes=num_processes, maxtasksperchild=1) as pool:
        # Pass output folder and a file number to each worker
        pool.starmap(process_file, [(file_path, output_folder, i + 1) for i, file_path in enumerate(parquet_files)])

def main():
    # Set up the argument parser
    parser = argparse.ArgumentParser(description="Process Parquet files and generate UUIDs.")
    parser.add_argument('input_folder', type=str, help="Path to the folder with input Parquet files.")
    parser.add_argument('output_folder', type=str, help="Path to the folder to save output Parquet files.")
    parser.add_argument('--num_processes', type=int, default=32, help="Number of processes to use for parallel processing.")

    args = parser.parse_args()

    # Make sure output folder exists
    os.makedirs(args.output_folder, exist_ok=True)

    # Process the Parquet files
    process_parquet_files(args.input_folder, args.output_folder, args.num_processes)

if __name__ == "__main__":
    main()
