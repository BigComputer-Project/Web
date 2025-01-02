import asyncio
import logging
import os
from datetime import datetime
from typing import Optional, List, Dict
from dataclasses import dataclass
from pathlib import Path
import time

from langchain_openai import ChatOpenAI
from playwright.async_api import async_playwright, Browser, Page, BrowserContext
from PIL import Image
from io import BytesIO

@dataclass
class WebsiteVisit:
    url: str
    screenshot_path: str
    timestamp: datetime
    task: str
    task_results: Optional[dict] = None
    html_content: Optional[str] = None

class AutoWebAgent:
    def __init__(
        self,
        api_key: str,
        output_dir: str = "web_agent_output",
        headless: bool = True,
        model: str = "gpt-4",
        max_pages_per_domain: int = 5,
        screenshot_width: int = 1920,
        screenshot_height: int = 1080
    ):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.headless = headless
        self.llm = ChatOpenAI(api_key=api_key, model=model)
        self.max_pages_per_domain = max_pages_per_domain
        self.width = screenshot_width
        self.height = screenshot_height
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.visited_urls: Dict[str, List[WebsiteVisit]] = {}
        self.logger = self._setup_logger()

        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger("AutoWebAgent")
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Console handler
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # File handler
        fh = logging.FileHandler(self.output_dir / "agent.log")
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        return logger

    async def initialize(self):
        """Initialize the browser and context"""
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            viewport={'width': self.width, 'height': self.height}
        )

    async def close(self):
        """Clean up resources"""
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()

    async def generate_task(self, url: str) -> str:
        """Generate an exploration task for the given URL"""
        prompt = f"""Given the URL: {url}
        Generate a specific task to explore this website. The task should be focused on:
        1. Gathering information about the main content
        2. Exploring key features or functionality
        3. Identifying important sections or pages

        Return only the task description, nothing else."""

        response = await self.llm.ainvoke(prompt)
        return response.content

    async def execute_task(self, url: str, task: str) -> dict:
        """Execute the generated task on the webpage by performing actual browser interactions"""
        if not self.context:
            raise RuntimeError("Browser context not initialized")

        page = await self.context.new_page()

        try:
            # Navigate to the URL and take initial screenshot
            await page.goto(url, wait_until="networkidle", timeout=30000)
            initial_screenshot = await self.take_screenshot(page, url, f"{task} - initial")

            results = []
            screenshots = [initial_screenshot]

            # Generate action plan using LLM
            action_plan = await self.generate_action_plan(task)

            for action in action_plan:
                try:
                    if action["type"] == "click":
                        element = await page.wait_for_selector(action["selector"])
                        await element.click()
                        # Take screenshot after click
                        screenshot = await self.take_screenshot(page, url, f"{task} - after_click_{action['selector']}")
                        screenshots.append(screenshot)

                    elif action["type"] == "input":
                        element = await page.wait_for_selector(action["selector"])
                        await element.type(action["text"])
                        # Take screenshot after input
                        screenshot = await self.take_screenshot(page, url, f"{task} - after_input_{action['selector']}")
                        screenshots.append(screenshot)

                    elif action["type"] == "scroll":
                        await page.evaluate("window.scrollBy(0, arguments[0])", action["pixels"])
                        # Take screenshot after scroll
                        screenshot = await self.take_screenshot(page, url, f"{task} - after_scroll_{action['pixels']}")
                        screenshots.append(screenshot)

                    elif action["type"] == "extract":
                        elements = await page.query_selector_all(action["selector"])
                        extracted_text = [await el.text_content() for el in elements]
                        results.append({
                            "action": "extract",
                            "selector": action["selector"],
                            "content": extracted_text
                        })

                    # Wait for any dynamic content to load
                    await page.wait_for_timeout(1000)

                except Exception as e:
                    results.append({
                        "action": action["type"],
                        "status": "failed",
                        "error": str(e)
                    })

            return {
                "task": task,
                "results": results,
                "screenshots": [s.screenshot_path for s in screenshots],
                "completed": True
            }

        except Exception as e:
            return {
                "task": task,
                "error": str(e),
                "completed": False
            }

        finally:
            await page.close()

    async def generate_action_plan(self, task: str) -> list:
        """Generate a sequence of actions to complete the task"""
        prompt = f"""Given the task: {task}
        Generate a sequence of browser actions needed to complete this task.
        Each action should be one of:
        - click: specify CSS selector or XPath to click
        - input: specify selector and text to type
        - scroll: specify pixels to scroll
        - extract: specify selector for information to extract

        Return the actions as a structured list that can be executed."""

        response = await self.llm.ainvoke(prompt)

        # Parse LLM response into structured actions
        # This is a simplified example - you'd need proper parsing logic
        actions = [
            {"type": "click", "selector": "#submit-button"},
            {"type": "input", "selector": "#search-input", "text": "search term"},
            {"type": "scroll", "pixels": 500},
            {"type": "extract", "selector": ".result-item"}
        ]

        return actions

    async def take_screenshot(self, page: Page, url: str, task: str) -> WebsiteVisit:
        """Take a screenshot of the current page"""
        timestamp = datetime.now()
        filename = f"{timestamp.strftime('%Y%m%d_%H%M%S')}_{url.replace('/', '_')[:100]}.png"
        screenshot_path = str(self.output_dir / filename)

        # Take screenshot
        screenshot_bytes = await page.screenshot(full_page=True)
        screenshot = Image.open(BytesIO(screenshot_bytes))
        screenshot.save(screenshot_path, format="PNG")

        # Get HTML content
        html_content = await page.content()

        return WebsiteVisit(
            url=url,
            screenshot_path=screenshot_path,
            timestamp=timestamp,
            task=task,
            html_content=html_content
        )

    async def explore_url(self, url: str):
        """Explore a single URL"""
        if not self.context:
            raise RuntimeError("Browser context not initialized")

        self.logger.info(f"Exploring URL: {url}")

        try:
            # Create new page and navigate
            page = await self.context.new_page()
            await page.goto(url, wait_until="networkidle", timeout=30000)

            # Generate task for this URL
            task = await self.generate_task(url)
            self.logger.info(f"Generated task: {task}")

            # Execute the task
            task_result = await self.execute_task(url, task)
            self.logger.info(f"Task execution result: {task_result}")

            # Take screenshot and record visit
            visit = await self.take_screenshot(page, url, task)
            visit.task_result = task_result  # Add task result to visit record

            # Store visit information
            domain = url.split('/')[2]
            if domain not in self.visited_urls:
                self.visited_urls[domain] = []
            self.visited_urls[domain].append(visit)

            # Extract new URLs to explore
            new_urls = await page.evaluate("""() => {
                const links = Array.from(document.getElementsByTagName('a'));
                return links.map(link => link.href).filter(href => href.startsWith('http'));
            }""")

            await page.close()
            return new_urls

        except Exception as e:
            self.logger.error(f"Error exploring {url}: {str(e)}")
            return []

    async def run(self, start_url: str, max_total_pages: int = 50):
        """Main exploration loop"""
        await self.initialize()

        try:
            urls_to_visit = [start_url]
            total_visited = 0

            while urls_to_visit and total_visited < max_total_pages:
                current_url = urls_to_visit.pop(0)
                domain = current_url.split('/')[2]

                # Skip if we've visited too many pages from this domain
                if len(self.visited_urls.get(domain, [])) >= self.max_pages_per_domain:
                    continue

                # Explore current URL and get new URLs
                new_urls = await self.explore_url(current_url)
                total_visited += 1

                # Add new URLs to visit
                urls_to_visit.extend([url for url in new_urls if url not in urls_to_visit])

                self.logger.info(f"Visited {total_visited} pages. Queue size: {len(urls_to_visit)}")

        finally:
            await self.close()
            self._save_report()

    def _save_report(self):
        """Save exploration report"""
        report_path = self.output_dir / "exploration_report.txt"
        with open(report_path, "w") as f:
            f.write("Web Exploration Report\n")
            f.write("====================\n\n")
            for domain, visits in self.visited_urls.items():
                f.write(f"\nDomain: {domain}\n")
                f.write("-" * (len(domain) + 8) + "\n")
                for visit in visits:
                    f.write(f"\nURL: {visit.url}\n")
                    f.write(f"Task: {visit.task}\n")
                    f.write(f"Task Result: {visit.task_result}\n")
                    if visit.task_result and "screenshots" in visit.task_result:
                        f.write("Screenshots taken during task:\n")
                        for screenshot in visit.task_result["screenshots"]:
                            f.write(f"- {screenshot}\n")
                    f.write(f"Timestamp: {visit.timestamp}\n")
                    f.write("\n")

# Example usage
async def main():
    agent = AutoWebAgent(
        api_key="openai_api_key",
        headless=True,  # Set to True for production
        output_dir="web_exploration_results"
    )

    await agent.run(
        start_url="https://example.com",
        max_total_pages=10
    )

if __name__ == "__main__":
    asyncio.run(main())
