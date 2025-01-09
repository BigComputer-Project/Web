import os
import time
import json
import asyncio
import argparse
import aiohttp
import traceback

from playwright.async_api import Error


from mmstack_web_crawler.utils import setup_logger
from mmstack_web_crawler.persistence import FileStorage
from mmstack_web_crawler.crawler import MMStackWebCrawler

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true', help='Enable debug mode')
    parser.add_argument("--storage", type=str, default="data", help="Path to store the crawled data")
    parser.add_argument("--task_address", type=str, default="http://localhost:8000/task", help="Address of the task message queue")
    parser.add_argument("--result_address", type=str, default="http://localhost:8000/done", help="Address of the result message queue")
    parser.add_argument("--max_pages", type=int, default=50, help="Maximum number of pages to crawl")
    parser.add_argument("--run_name", type=str, default=None, help="Name of the run")
    parser.add_argument("--restart_interval", type=int, default=1000, help="Interval to restart the browser")

    args = parser.parse_args()
    if not args.run_name:
        args.run_name = f"worker_{time.strftime('%Y%m%d-%H%M%S')}"

    return args


def handle_task_exception(loop, context):
    """Global exception handler for the event loop."""
    exception = context.get("exception")  # The actual exception object
    if exception:
        logger.error("Caught exception in the event loop:", exc_info=exception)
        loop.stop()
    else:
        logger.error("Unhandled error context:", context)
        loop.stop()


async def worker(task, crawler, storage):
    async def send_with_timeout(message, timeout=10):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(args.result_address, json=message, timeout=timeout) as response:
                    if response.status != 200:
                        print(f"Error while sending message: {message}")
        except asyncio.TimeoutError:
            print(f"Timeout while sending message: {message}")
        except aiohttp.ClientError as e:
            print(f"Aiohttp error while sending message: {e}")

    # Call the crawl_page function to process the URL
    crawled_content = await crawler.crawl(task["url"], output_annotated_screenshot=False)

    # Send the result back to the server
    if crawled_content:
        await storage.save({
            "id": task["id"],
            "url": task["url"],
            "content": crawled_content
        })
    await send_with_timeout({
        "id": task["id"],
        "type": "complete",
    })


def get_storage(storage):
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    return FileStorage(base_path=os.path.join(storage, timestamp))


async def fetch_job():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(args.task_address) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print("Error fetching task")
                    return None
    except aiohttp.ClientError as e:
        print(f"Aiohttp error while fetching task: {e}")
        return None



async def worker_main():
    while True:
        tasks = []

        # Initialize the crawler
        async with MMStackWebCrawler(logger=logger, headless=True, max_pages=args.max_pages) as crawler:
            logger.info("Browser initialized.")
            while len(tasks) < args.restart_interval:
                # Wait for capacity
                await crawler.wait_for_capacity()
                # Receive a URL from the queue
                task = await fetch_job()
                if task is None:
                    logger.info("No more tasks. Waiting...")
                    await asyncio.sleep(3)
                    continue
                logger.info("Task received: ", task)
                # Call the crawl_page function to process the URL
                task = asyncio.create_task(worker(task, crawler, storage))
                tasks.append(task)
            logger.info("Maximum taks per broweser reached. Waiting for tasks to complete...")
            await asyncio.gather(*tasks)
        logger.info("Restarting the browser...")


if __name__ == '__main__':
    args = parse_args()
    logger = setup_logger("worker", loglevel="debug" if args.debug else "warning")

    storage = get_storage(args.storage)

    print("Worker started. Waiting for jobs...")

    loop = asyncio.new_event_loop()
    loop.set_exception_handler(handle_task_exception)
    loop.run_until_complete(worker_main())
    loop.close()
