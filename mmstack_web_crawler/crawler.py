import asyncio
import uuid
from io import BytesIO
from PIL import Image, ImageDraw
from playwright.async_api import async_playwright, Error as PlaywrightError
from bs4 import BeautifulSoup
import uuid

from mmstack_web_crawler.browser_handler import ChromeHandler, PageHandler
from mmstack_web_crawler.utils import mark_box_on_screenshot


class MMStackWebCrawler:
    def __init__(self, logger=None, headless=True, max_pages=50):
        self.logger = logger
        self.headless = headless
        self.max_pages = max_pages

        self.browser_handler = None

    async def initialize(self, headless=True):
        self.browser_handler = ChromeHandler(width=1920, height=1080, wait_timeout=5000, logger=self.logger, headless=self.headless)
        await self.browser_handler.build_driver()

    async def __aenter__(self):
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def save_screenshot(self, url, output_path=None):
        screenshot_image = await self.screenshot()

        if output_path:
            screenshot_image.save(output_path, format="AVIF")
            if self.logger:
                self.logger.info(f"Screenshot saved to {output_path}")

        return screenshot_image
    


    async def mark_all_bounding_boxes_in_body(self, page_handler):
        """Marks all elements within the <body> with their bounding box attributes."""
        try:
            await page_handler.page.evaluate(
                """
                (function markAllBBoxesAndJoinPositions() {
                    const scrollX = window.scrollX;
                    const scrollY = window.scrollY;

                    const elements = document.body.querySelectorAll('*');

                    elements.forEach((element) => {
                        const rect = element.getBoundingClientRect();
                        // Absolute position in the document (accounting for scroll offsets)
                        const bbox = `(${Math.round(rect.left + scrollX)},${Math.round(rect.top + scrollY)},${Math.round(rect.right + scrollX)},${Math.round(rect.bottom + scrollY)})`;
                        // If it's not all zero, set attribute
                        if (bbox !== '(0,0,0,0)') {
                            element.setAttribute('__bbox__', bbox);
                        }
                    });
                })();
                """
            )
            return True
        except Exception as e:
            self.logger.error(f"Crawler error while marking bounding boxes: {e}")
            return False
        
    async def erase_marks_in_body(self, page_handler):
        """Erases all bounding box attributes from elements within the <body>, handling cases where they may not exist."""
        try:
            await page_handler.page.evaluate(
                """
                () => {
                    const elements = document.body.querySelectorAll('*');
                    elements.forEach((element) => {
                        const attrs = ['__bbox__', '__tbox__'];
                        attrs.forEach(attr => {
                            if (element.hasAttribute(attr)) {
                                element.removeAttribute(attr);
                            }
                        });
                    });
                }
                """
            )
            return True
        except Exception as e:
            self.logger.error(f"Crawler error while erasing bounding boxes markers: {e}")
            return False

    def prune_html_by_visibility(self, html_content, viewport_bbox):
        """Remove hidden elements from the HTML content."""
        soup = BeautifulSoup(html_content, "html.parser")

        view_top, view_left, view_bottom, view_right = viewport_bbox

        def try_get_attr(element, attr):
            try:
                return element.get(attr)
            except (KeyError, AttributeError):
                return None

        for element in soup.find_all():
            real_bbox = try_get_attr(element, "__bbox__")
            top, left, bottom, right = map(int, real_bbox.strip("()").split(",")) if real_bbox else (None, None, None, None)

            if not all([top, left, bottom, right]):
                continue
            # check if the at least a part of the element is visible in the viewport: iou > 0
            if float(top) < view_bottom and float(bottom) > view_top and float(left) < view_right and float(right) > view_left:
                continue
            else:
                element.decompose()
        return str(soup)
    
    async def dump_ui_and_html_with_bbox(self, page_handler, mark_position=True):
        """Capture the UI screenshot and HTML content after the page is loaded."""
    
        screenshot_image = await page_handler.screenshot()

        if mark_position:
            await self.mark_all_bounding_boxes_in_body(page_handler)
            html_content = await page_handler.dump_html()
            await self.erase_marks_in_body(page_handler)
        else:
            html_content = await page_handler.dump_html()

        # Return both HTML and screenshot
        return html_content, screenshot_image

    # Crawling logic
    async def wait_for_capacity(self):
        while True:
            num_current_pages = self.browser_handler.count_pages()
            if num_current_pages >= self.max_pages:
                await asyncio.sleep(1)
            else:
                break


    async def crawl(self, url, output_annotated_screenshot=False):
        await self.wait_for_capacity()

        try:
            async with await self.browser_handler.new_page(url) as page_handler:
                response_code = await page_handler.access_url(url, timeout=15)
                if response_code not in [200, 302]:
                    self.logger.info(f"Failed to access {url} with response code {response_code}")
                    return None
                await asyncio.sleep(5)

                # Extend the page to full height based on content height
                await page_handler.extend_to_full_height()
                await asyncio.sleep(5)
                html_content, screenshot_image = await self.dump_ui_and_html_with_bbox(page_handler, mark_position=True)
                result = {
                    "url": url,
                    "html": html_content,
                    "image": screenshot_image,
                }

                if output_annotated_screenshot:
                    result["annotated_image"] = mark_box_on_screenshot(screenshot_image, html_content)
        except PlaywrightError as e:
            self.logger.info(f"Error while crawling {url}: {e}")
            result = None
        

        return result

        
    async def close(self):
        await self.browser_handler.close()
        
    

