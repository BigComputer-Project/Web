import asyncio
import uuid
from io import BytesIO
from playwright.async_api import async_playwright
from playwright._impl._errors import (
    Error as PlaywrightError,
    TimeoutError,
)

from PIL import Image


class ChromeHandler:
    def __init__(self, width=1920, height=1080, wait_timeout=5000, logger=None, headless=False):
        self.id = str(uuid.uuid4())  # Generate a unique ID for each crawler
        self.width = width
        self.height = height
        self.wait_timeout = wait_timeout
        self.headless = headless
        self.browser = None
        self.context = None
        self.page_handlers = {}  # A dictionary to hold PageHandler instances by ID
        self.logger = logger

    async def build_driver(self):
        playwright = await async_playwright().start()
        browser_args = [
            "--ignore-certificate-errors",
            "--disable-logging",
            "--disable-gpu",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--charset=utf-8",
            "--disable-application-cache",
            "--media-cache-size=0",
            "--disk-cache-size=0",
            "--log-level=3",
            "--silent",
        ]

        self.browser = await playwright.chromium.launch(headless=self.headless, args=browser_args)
        self.context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            viewport={"width": self.width, "height": self.height}
        )

        if self.logger:
            self.logger.info(f"Crawler {self.id} initialized with headless={self.headless}")
        else:
            print(f"Crawler {self.id} initialized with headless={self.headless}")

    async def close(self):
        try:
            # Close all pages and update the page_handlers dictionary
            for page_id, page_handler in list(self.page_handlers.items()):
                await page_handler.close()
                self._remove_page(page_id)
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
        except Exception as e:
            if self.logger:
                self.logger.error(f"Crawler {self.id} encountered an error while shutting down: {e}")
            else:
                print(f"Crawler {self.id} encountered an error while shutting down: {e}")
        finally:
            if self.logger:
                self.logger.info(f"Crawler {self.id} shut down")
            else:
                print(f"Crawler {self.id} shut down")

    def _remove_page(self, page_id):
        """Helper function to safely remove a page from the dictionary."""
        if page_id in self.page_handlers:
            del self.page_handlers[page_id]

    # Async context manager methods
    async def __aenter__(self):
        await self.build_driver()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def new_page(self, context=None):
        if self.context:
            page_handler = PageHandler(self, None, self.logger) 
            # Take the spot to avoid the worker pulling too many tasks!
            self.page_handlers[page_handler.id] = page_handler  # Placeholder to reserve the spot
            # Wait for the new page to be created
            new_page = await self.context.new_page()
            page_handler.set_page(new_page)
            assert page_handler.page is not None
            return page_handler
        else:
            raise ValueError("No browser context is available. Please initialize the browser first.")

    def count_pages(self):
        return len(self.page_handlers)


class PageHandler:
    def __init__(self, browser_handler, page, logger=None):
        self.id = str(uuid.uuid4())  # Generate a unique ID for each page
        self.browser_handler = browser_handler  # The ChromeHandler instance
        self.page = page
        self.logger = logger
    
    def set_page(self, page):
        self.page = page

    async def close(self):
        try:
            if self.page:
                await self.page.close()  # Close the page
                if self.logger:
                    self.logger.info(f"Page {self.id} closed")
                else:
                    print(f"Page {self.id} closed")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error closing page {self.id}: {e}")
            else:
                print(f"Error closing page {self.id}: {e}")
        finally:
            # After closing, remove the page from the handler's page_handlers dictionary
            self.browser_handler._remove_page(self.id)

    # Async context manager methods
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def access_url(self, url, timeout=15):
        if self.logger:
            self.logger.info(f"Accessing URL: {url}")

        response_code = None

        def handle_response(response):
            nonlocal response_code
            if response_code is None:
                response_code = response.status


        try:
            self.page.on('response', handle_response)
            await self.page.goto(url, wait_until="load", timeout=timeout * 1000)  # or "networkidle"
        except TimeoutError:
            if self.logger:
                self.logger.info(f"Timed out while accessing URL: {url}")
            return None
        except PlaywrightError as e:
            if self.logger:
                self.logger.error(f"Error accessing URL: {url} - {e}")
            return None

        return response_code

    async def dump_html(self):
        return await self.page.content()

    async def screenshot(self):
        screenshot_bytes = await self.page.screenshot()
        image = Image.open(BytesIO(screenshot_bytes))
        return image

    async def get_scroll_position(self):
        scroll_x = await self.page.evaluate('window.scrollX')
        scroll_y = await self.page.evaluate('window.scrollY')
        return scroll_x, scroll_y

    async def set_scroll_position(self, scroll_x, scroll_y):
        await self.page.evaluate(f"window.scrollTo({scroll_x}, {scroll_y})")

    async def extend_to_full_height(self):
        full_page_height = await self.page.evaluate("document.documentElement.scrollHeight")
        full_page_height = min(full_page_height, 16384)  # Limit the height
            
        await self.page.set_viewport_size({"width": self.page.viewport_size["width"], "height": full_page_height})
        # Trigger scroll and resize events to update the page in case of lazy loading
        await self.page.evaluate("""() => {
            window.dispatchEvent(new Event('scroll'));
            window.dispatchEvent(new Event('resize'));
        };""")


    # Element selection
    async def find_all_hidden_elements_by_attr(self):
        # Find elements with display:none or visibility:hidden
        elements = self.page.locator("//*[contains(@style,'display:none') or contains(@style,'visibility:hidden')]")
        element_handles = await elements.element_handles()  # Get element handles asynchronously
        return element_handles

    async def find_all_visible_elements(self):
        # Find all elements on the page
        elements = self.page.locator("//*")
        visible_elements = []
        for element in await elements.element_handles():  # Use async for to handle async iterables
            if element.is_visible():  # Check if each element is visible
                visible_elements.append(element)
        return visible_elements

    async def find_all_clickable_elements(self):
        # Find all clickable elements
        elements = self.page.locator("//a | //button | //input[@type='submit'] | //*[@onclick]")
        clickable_elements = []
        for element in await elements.element_handles():
            clickable_elements.append(element)
        return clickable_elements

    async def find_all_titled_elements(self):
        # Find all elements with the 'title' attribute
        elements = self.page.locator("//*[@title]")
        element_handles = await elements.element_handles()  # Get element handles asynchronously
        return element_handles

    async def is_leaf_element(self, element):
        # Check if an element has no child elements
        child_elements = await element.locator(".//*").count()  # Use count() to get the number of child elements
        return child_elements == 0

    async def find_all_alt_elements(self):
        # Find elements with the 'alt' attribute
        elements = self.page.locator("//*[@alt]")
        element_handles = await elements.element_handles()  # Get element handles asynchronously
        return element_handles

    async def find_all_aria_label_elements(self):
        # Find elements with the 'aria-label' attribute
        elements = self.page.locator("//*[@aria-label]")
        element_handles = await elements.element_handles()  # Get element handles asynchronously
        return element_handles

    
    def locate_element(self, element, scroll_x=None, scroll_y=None):
        # Get the bounding box of the element
        bbox = element.bounding_box()

        if not bbox:
            return None

        # If scroll_x or scroll_y are not passed, retrieve them from the page
        if scroll_x is None or scroll_y is None:
            scroll_x, scroll_y = self.page.evaluate('window.scrollX, window.scrollY')

        # Adjust the bounding box based on the scroll position
        left = bbox['x'] - scroll_x
        top = bbox['y'] - scroll_y
        right = left + bbox['width']
        bottom = top + bbox['height']

        text = element.inner_text() or element.get_attribute("value") or ""

        # Return the element's coordinates and text
        return {
            "text": text,
            "top": top,
            "left": left,
            "right": right,
            "bottom": bottom,
            # Unadjusted coordinates
            "real_top": bbox['y'],
            "real_left": bbox['x'],
            "real_right": bbox['x'] + bbox['width'],
            "real_bottom": bbox['y'] + bbox['height'],
        }