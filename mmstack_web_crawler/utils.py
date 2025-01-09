import os
import logging
from bs4 import BeautifulSoup
from PIL import Image, ImageDraw

import logging

def setup_logger(logname, loglevel, run_name='app'):
    if loglevel == 'info':
        level = logging.INFO
    elif loglevel == 'debug':
        level = logging.DEBUG
    elif loglevel == 'warning':
        level = logging.WARNING
    elif loglevel == 'error':
        level = logging.ERROR
    elif loglevel == 'critical':
        level = logging.CRITICAL
    else:
        level = logging.INFO

    # Create a logger
    logger = logging.getLogger(logname)
    logger.setLevel(level)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_formatter = logging.Formatter(
        "%(asctime)s - [%(process)d] - %(levelname)s - %(filename)s - %(lineno)d - %(message)s"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # Create a file handler
    file_handler = logging.FileHandler(f"{run_name}.log")
    file_handler.setLevel(level)
    file_formatter = logging.Formatter(
        "%(asctime)s - [%(process)d] - %(levelname)s - %(filename)s - %(lineno)d - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger




def mark_box_on_screenshot(screenshot, html_content, screenshot_bbox=None):
    """Mark bounding boxes on the screenshot."""
    if screenshot_bbox:
        sx1, sy1, sx2, sy2 = screenshot_bbox[0], screenshot_bbox[1], screenshot_bbox[2], screenshot_bbox[3]
    else:
        # Default to the full screenshot
        sx1, sy1, sx2, sy2 = 0, 0, screenshot.width, screenshot.height

    image = screenshot.copy()
    draw = ImageDraw.Draw(image)
    soup = BeautifulSoup(html_content, "html.parser")

    for element in soup.find_all():
        bbox = element.get("__bbox__")
        left, top, right, bottom = map(int, bbox.strip("()").split(",")) if bbox else (None, None, None, None)

        if not all([left, top, right, bottom]):
            continue
        
        if left == top == right == bottom == 0:  # The element does not have a bounding box
            continue

        # Check if the element is visible in the viewport
        if not (top < sy2 and bottom > sy1 and left < sx2 and right > sx1):
            continue

        draw.rectangle([left, top, right, bottom], outline="red", width=2)
        # add text: tag name
        draw.text((left, top), element.name, fill="red")
    
    return image