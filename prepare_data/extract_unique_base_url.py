import os
import pandas as pd
import random
from urllib.parse import urlparse
from tqdm import tqdm
import argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Group rows by hostname and save random samples to batches.")
    parser.add_argument("--input_folder", required=True, help="Path to the folder containing Parquet files.")
    parser.add_argument("--output_folder", required=True, help="Path to the folder to save output batches.")
    parser.add_argument("--batch_size", type=int, default=10000000, help="Number of rows per batch (default: 10000000).")
    args = parser.parse_args()

    # Get list of Parquet files
    file_paths = [
        os.path.join(root, file)
        for root, _, files in os.walk(args.input_folder)
        for file in files if file.endswith(".parquet")
    ]

    grouped_rows = {}

    # Process files sequentially
    for file_path in tqdm(file_paths, desc="Processing Files"):
        df = pd.read_parquet(file_path)
        for _, row in tqdm(df.iterrows(), leave=False, total=len(df), desc=f"Processing {file_path}"):
            # Manually extract the hostname from the URL
            hostname = urlparse(row['url']).hostname
            
            record = {
                "uuid": row['uuid'],
                "url": row['url']
            }
            
            if hostname not in grouped_rows:
                grouped_rows[hostname] = []
            grouped_rows[hostname].append(record)

    os.makedirs(args.output_folder, exist_ok=True)

    # Select random rows from each group
    all_rows = [
        random.choice(rows)
        for rows in tqdm(grouped_rows.values(), desc="Selecting Random Rows")
    ]
    
    # Save random rows to batches
    for i in tqdm(range(0, len(all_rows), args.batch_size), desc="Saving Batches"):
        batch = all_rows[i:i+args.batch_size]
        batch_df = pd.DataFrame(batch)
        # Set uuid as index
        batch_df.set_index("uuid", inplace=True)
        batch_file = os.path.join(args.output_folder, f"batch_{i // args.batch_size + 1}.parquet")
        batch_df.to_parquet(batch_file, index=True)
        print(f"Saved batch to {batch_file}")