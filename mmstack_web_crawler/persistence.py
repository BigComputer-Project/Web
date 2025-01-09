import os
import json
import asyncio
import aiofiles
import asyncio

from pathlib import Path

from PIL import Image, ImageDraw
import pillow_avif

async def save_image_async(image: Image.Image, image_path: str, format: str = "AVIF"):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, image.save, image_path, format)

        
class FileStorage:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.jsonl_file = self.base_path / "data.jsonl"

    async def save(self, data: dict):
        # Validate input data
        required_keys = {"image", "html"}
        if not all(key in data["content"] for key in required_keys):
            raise ValueError("Missing required keys in data")

        # Create a directory for the given id
        task_id = data["id"]
        task_dir = self.base_path / str(task_id)
        task_dir.mkdir(parents=True, exist_ok=True)

        # Save the image file
        image_path = task_dir / f"{task_id}.png"
        await save_image_async(data["content"]["image"], image_path, "PNG")
        
        if "annotated_image" in data["content"]:
            annotated_image_path = task_dir / f"{task_id}_annotated.png"
            await save_image_async(data["content"]["annotated_image"], annotated_image_path, "PNG")

        # Save the html file
        html_path = task_dir / f"{task_id}.html"
        async with aiofiles.open(html_path, 'w') as html_file:
            await html_file.write(data["content"]["html"])

        # Append to the jsonl file
        jsonl_data = {"id": task_id, "url": data["url"]}
        with open(self.jsonl_file, "a") as jsonl:
            jsonl.write(json.dumps(jsonl_data) + "\n")

        print(f"Saved data for id: {task_id}")