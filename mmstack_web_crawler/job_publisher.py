import os
import json
import time
import hashlib
import logging
import httpx
import pandas as pd
import asyncio
from pathlib import Path
import argparse
from tqdm.asyncio import tqdm
from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse, Response
import uvicorn

# FastAPI app for worker communication
app = FastAPI()

pending_tasks = {}

def calculate_checksum(file_path):
    """Calculate the checksum of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def save_checkpoint(checkpoint_data, checkpoint_file):
    """Save progress checkpoint to a JSON file."""
    with open(checkpoint_file, "w") as f:
        json.dump(checkpoint_data, f)


def parquet_data_generator(parquet_files_with_indices):
    for parquet_file, df, start_index in parquet_files_with_indices:
        for index in range(start_index, len(df)):
            yield parquet_file, index, df.iloc[index]


def task_result_callback():
    pbar.update(1)
    pbar.set_postfix({"Working": len(tasks_in_progress)})
    pbar.refresh()


def load_from_checkpoint(parquet_folder, checkpoint_file):
    # Load checkpoint
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            checkpoint = json.load(f)
    else:
        checkpoint = {}

    parquet_files = sorted([f for f in os.listdir(parquet_folder) if f.endswith('.parquet')])
    pending_data = []
    num_tasks = 0
    for parquet_file in parquet_files:

        # Calculate the file checksum
        print(f"Loading file: {parquet_file}")
        real_path = os.path.join(parquet_folder, parquet_file)
        checksum = calculate_checksum(real_path)
        parquet = pd.read_parquet(real_path)

        # Skip finished files
        if (
            parquet_file in checkpoint and 
            checkpoint[parquet_file]["checksum"] == checksum and
            checkpoint[parquet_file]["progress"] == len(parquet)
        ):
            print(f"Skipping finished file: {parquet_file}")
            continue

        # Load progress or initialize checkpoint for the file
        if parquet_file in checkpoint and checkpoint[parquet_file]["checksum"] == checksum:
            start_index = checkpoint[parquet_file]["progress"]
            print(f"Found checkpoint for file: {parquet_file}, will resume from index {start_index}")
        else:
            start_index = 0
            checkpoint[parquet_file] = {"checksum": checksum, "progress": 0}

        num_tasks += len(parquet) - start_index
        
        # Append the file and start index pair for the generator
        pending_data.append((parquet_file, parquet, start_index))

    print(f"Found {num_tasks} pending tasks.")

    return parquet_data_generator(pending_data), checkpoint, num_tasks

last_save_time = time.time()
def possibly_save_checkpoint(checkpoint, checkpoint_file):
    global last_save_time
    current_time = time.time()
    if current_time - last_save_time > args.save_interval:
        save_checkpoint(checkpoint, checkpoint_file)

@app.get("/task")
async def get_task(request: Request):
    """Handle task request from worker."""
    try:
        parquet_file, index_in_file, task_data = next(data_loader)
    except StopIteration:
        print("All tasks completed.")
        exit(0)

    uuid = task_data.name
    task = {
        "id": uuid,
        "url": task_data["url"],
    }

    tasks_in_progress[uuid] = {
        "timestamp": time.time(),
        "id": uuid,
        "url": task_data["url"],
    }

    checkpoint[parquet_file]["progress"] = index_in_file
    possibly_save_checkpoint(checkpoint, args.checkpoint_file)

    return JSONResponse(content=task)


@app.post("/done")
async def acknowledge_task(request: Request):
    """Acknowledge that a worker has completed a task."""
    # get json data in sync
    ack_data = await request.json()
    task_id = ack_data["id"]
    logging.info(f"Received acknowledgment for task {task_id}")

    # Clean up queue
    if task_id in tasks_in_progress:
        del tasks_in_progress[task_id]
    else:
        logging.warning(f"Received acknowledgment for unknown task {task_id}. Maybe it timed out.")
    
    task_result_callback()

    return Response(status_code=200)


def parse_args():
    parser = argparse.ArgumentParser(description="Publish tasks from Parquet files.")
    parser.add_argument("--parquet_folder", type=str,
                        help="Path to the folder containing Parquet files.")
    parser.add_argument("--checkpoint_file", type=str, default="checkpoint.json",
                        help="Path to the JSON file for saving checkpoints.")
    parser.add_argument("--max_queue_size", type=int, default=2,
                        help="Maximum number of jobs allowed in the queue.")
    parser.add_argument("--url", type=str, default="localhost",
                        help="Publish address.")
    parser.add_argument("--port", type=int, default=10086,
                        help="Publish port.")
    parser.add_argument("--batch_size", type=int, default=100,
                        help="Number of URLs to publish before saving progress.")
    parser.add_argument("--worker_timeout", type=int, default=300)
    parser.add_argument("--save_interval", type=int, default=100)

    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()

    # Global variable to store the length of the queue
    tasks_in_progress = {}
    
    # Load resume state from checkpoint
    data_loader, checkpoint, num_tasks = load_from_checkpoint(args.parquet_folder, args.checkpoint_file)

    pbar = tqdm(total=num_tasks, desc="Publishing tasks")

    uvicorn.run(app, host=args.url, port=args.port, log_level="critical")
