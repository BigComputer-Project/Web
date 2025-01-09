import asyncio
from mmstack_web_crawler.utils import setup_logger
from mmstack_web_crawler.crawler import MMStackWebCrawler

async def worker():
    # Set up logger directly
    logger = setup_logger("hi", "debug")  # Change to "warning" if not in debug mode

    # URL to crawl (specify the URL here)
    url = "http://www.example.com"  # Replace with the desired URL

    # Initialize the crawler
    async with MMStackWebCrawler(logger=logger, headless=True) as crawler:
        # Crawl the specified URL
        await crawler.crawl(url, output_annotated_screenshot=True)

if __name__ == '__main__':
    # Run the main function using asyncio
    asyncio.run(worker())
