from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from deprecated import deprecated
# import requests
from PIL import Image, ImageDraw
from selenium.common.exceptions import TimeoutException
import os
import time

import re
import logging
import traceback

from utils import generate_url_hash

import argparse


class CrawlerBase:
    def __init__(self, driver_path, width=1920, height=1080, wait_timeout=5, logger=None, nogui=False):
        self.driver_path = driver_path
        self.width = width
        self.height = height
        self.wait_timeout = wait_timeout
        self.nogui = nogui
        self.driver = self.buildDriver(driver_path, width, height, wait_timeout, nogui)
        self.logger = logger

    @staticmethod
    def buildDriver(driver_path, width, height, wait_timeout, nogui):
        service = webdriver.chrome.service.Service(executable_path=driver_path)
        chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument('--user-data-dir=/cpfs01/shared/public/wangyian/data/web_crawl/tmp')
        # print(chrome_options.arguments)
        chrome_options.add_argument('--ignore-certificate-errors')
        # chrome_options.add_argument('--user-data-dir=/cpfs01/shared/public/wangyian/data/web_crawl/tmp')
        chrome_options.add_argument('--disable-logging')
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')  # 避免沙箱模式
        chrome_options.add_argument('--disable-dev-shm-usage')  # 禁用/dev/shm的使用
        chrome_options.add_argument('--charset=utf-8')  # 设置字符编码为 UTF-8
        # 设置文件下载目录
        chrome_options.add_argument('--disk-cache-dir=/home/tiger/Desktop/seeclick-crawler/browser_data/cache')
        chrome_options.add_argument('--temp-directory=/home/tiger/Desktop/seeclick-crawler/browser_data/tmp')
        chrome_options.add_argument('--disable-application-cache')
        chrome_options.add_argument('--media-cache-size=0')
        chrome_options.add_argument('--disk-cache-size=0')
        
        chrome_options.add_argument('--log-level=3')
        chrome_options.add_argument('--silent')
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": "./downloads"
        })
        desired_capabilities = chrome_options.to_capabilities()
        desired_capabilities['pageLoadStrategy'] = 'none'
        desired_capabilities['timeouts'] = {'implicit': 3000, 'pageLoad': 3000, 'script': 3000}


        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.set_window_size(width, height)
        # print(chrome_options.arguments)
        driver.set_page_load_timeout(3500)
        driver.implicitly_wait(wait_timeout)  # 设置隐式等待时间
        return driver

    def restart(self):
        if self.logger:
            self.logger.info("restart driver")
        else:
            print("restart driver")
        self.driver.quit()
        self.driver = self.buildDriver(self.driver_path, self.width, self.height, self.wait_timeout, self.nogui)

    def quit(self):
        self.driver.quit()

    def accessURL(self, url):
        if self.logger:
            self.logger.info("access url: {}".format(url))
        else:
            print("access url: {}".format(url))
        self.driver.get(url)


class Crawler(CrawlerBase):
    def __init__(self, driver_path, img_dir, mhtml_dir, width, height, wait_timeout, logger=None, draw_box=False,
                 scrape_hover=False,
                 nogui=True):
        super().__init__(driver_path, width, height, wait_timeout, nogui=nogui, logger=logger)
        self.driver_path = driver_path
        self.width = width
        self.height = height
        self.wait_timeout = wait_timeout
        self.img_dir = img_dir
        self.draw_box = draw_box  # 是否需要draw_box
        self.scrape_hover = scrape_hover  # 是否需要检测hover的元素
        os.makedirs(img_dir, exist_ok=True)
        self.mhtml_dir = mhtml_dir
        os.makedirs(mhtml_dir, exist_ok=True)
        self.additional_timeout = 2
        self.box_color = (255, 0, 0)
        self.logger = logger

    def saveScreenshot(self, save_path):
        self.driver.save_screenshot(save_path)
        if self.logger:
            self.logger.info("save screenshot to {}".format(save_path))
        else:
            print("save screenshot to {}".format(save_path))
            
    def saveMHTML(self, save_path):
        mhtml = self.driver.execute_cdp_cmd('Page.captureSnapshot', {})
        with open(save_path, 'w', newline = '') as f:
            f.write(mhtml['data'])
            self.logger.info("save mhtml to {}".format(save_path))
            
    def findAllClickableElements(self):
        # elements = self.driver.find_elements(By.XPATH,
        #                                      "//a | //button | //input[@type='submit'] | //input[@type='button'] | //label | //*[@onclick]")
        elements = self.driver.find_elements(By.XPATH,
                                             "//a | //button | //input[@type='submit'] | //*[@onclick]")

        return elements

    def findAllTitledElements(self):
        elements = self.driver.find_elements(By.XPATH, "//*[@title]")
        return elements

    def findAllMouseOverableElements(self):
        # 1. method1
        # elements = self.driver.find_elements(By.XPATH, "//*[@onmouseover]")
        # 2. method2
        # 使用JavaScript执行脚本来获取所有绑定了mouseover事件的元素
        elements = self.driver.execute_script(
            'return Array.from(document.querySelectorAll("*")).filter(elem => {'
            '   const events = getEventListeners(elem);'
            '   return events.hasOwnProperty("mouseover");'
            '});'
        )
        return elements

    def findAllElements(self):
        """查找页面上的所有elements"""
        return self.driver.find_elements(By.XPATH, '//*')

    def findAllHiddenElements(self):
        elements = self.driver.find_elements(By.XPATH, "//*")
        hidden_elements = [element for element in elements if not element.is_displayed()]
        return hidden_elements

    def findAllNotHiddenElements(self):
        elements = self.driver.find_elements(By.XPATH, "//*")
        not_hidden_elements = [element for element in elements if element.is_displayed()]
        return not_hidden_elements

    def __processClickableElements(self):
        results = []
        signatures = set()
        elements = self.findAllClickableElements()
        for element in elements:
            # 判断当前元素是否是叶子节点
            try:
                left_top = (element.location['x'], element.location['y'])
                width, height = element.size['width'], element.size['height']
                right_bottom = (left_top[0] + width, left_top[1] + height)
                text = element.text
                if text is None or text == '':
                    text = element.get_attribute("value")
                if text is None or text == '':
                    continue
                if width == 0 or height == 0:
                    continue
                if right_bottom[0] >= self.width or right_bottom[1] >= self.height:
                    continue
                signature = f'{left_top[0]}-{left_top[1]}-{width}-{height}'
                if signature in signatures: continue
                signatures.add(signature)
                results.append({"left-top": left_top, "size": (width, height), "text": text, "type": "text"})
                # if self.logger:
                #     self.logger.info(
                #         f"location: ({left_top[0]}, {left_top[1]}), size: ({width}, {height}), text: {text}")
                # else:
                    # print(f"location: ({left_top[0]}, {left_top[1]}), size: ({width}, {height}), text: {text}")
            except Exception as exp:
                traceback.print_exc()
                continue
        return results

    def __processHoverElementsV2(self):
        results = []
        signatures = set()
        elements = self.findAllTitledElements()
        for element in elements:
            # 判断当前元素是否是叶子节点
            try:
                left_top = (element.location['x'], element.location['y'])
                width, height = element.size['width'], element.size['height']
                right_bottom = (left_top[0] + width, left_top[1] + height)
                text = element.text
                if not element.is_displayed():
                    continue
                if width == 0 or height == 0:
                    continue
                if right_bottom[0] >= self.width or right_bottom[1] >= self.height:
                    continue
                if text is None or text.strip() == '':
                    title = element.get_attribute("title")
                    if title is None or title == '': continue
                    signature = f'{left_top[0]}-{left_top[1]}-{width}-{height}'
                    if signature in signatures: continue
                    signatures.add(signature)
                    results.append({"left-top": left_top, "size": (width, height), "text": title, "type": "hover"})
                    # if self.logger:
                    #     self.logger.info(
                    #         f"location: ({left_top[0]}, {left_top[1]}), size: ({width}, {height}), text: {title}")
                    # else:
                    #     print(f"location: ({left_top[0]}, {left_top[1]}), size: ({width}, {height}), text: {title}")
            except Exception as exp:
                traceback.print_exc()
                continue
        return results

    @deprecated(reason="use __processHoverElementsV2 instead")
    def __processHoverElements(self):
        elements = self.findAllNotHiddenElements()
        # 获取悬停前的页面源代码
        before_hover_page_source = self.driver.page_source
        # 创建ActionChains对象
        not_hidden_elements = self.findAllNotHiddenElements()
        actions = ActionChains(self.driver)
        results = []
        signatures = set()
        for element in elements:
            # 判断当前元素是否是叶子节点
            try:
                is_leaf_element = self.isLeafElement(element)
                if not is_leaf_element:
                    continue
                left_top = (element.location['x'], element.location['y'])
                width, height = element.size['width'], element.size['height']
                right_bottom = (left_top[0] + width, left_top[1] + height)
                if not element.is_displayed():
                    continue
                if width == 0 or height == 0:
                    continue
                if right_bottom[0] >= self.width or right_bottom[1] >= self.height:
                    continue
                signature = f'{left_top[0]}-{left_top[1]}-{width}-{height}'
                if signature in signatures: continue
                signatures.add(signature)
                # actions.reset_actions()
                actions.move_to_element(element).perform()
                after_hover_page_source = self.driver.page_source
                if after_hover_page_source != before_hover_page_source:
                    not_hidden_elements_now = self.findAllNotHiddenElements()
                    display_elements = set(not_hidden_elements_now) - set(not_hidden_elements)
                    tips = []
                    if len(display_elements) > 0:
                        # print('================================================================')
                        # print(f"location: ({left_top[0]}, {left_top[1]}), size: ({width}, {height})")
                        for display_element in display_elements:
                            if not self.isLeafElement(display_element): continue
                            if display_element.text is None or display_element.text.strip() == '': continue
                            # print(display_element.text)
                            tips.append(display_element.text)
                        if len(tips) > 0:
                            sep = '@@'
                            text = sep.join(tips)
                            results.append({"left-top": left_top, "size": (width, height), "text": text})
            except Exception as exp:
                traceback.print_exc()
                continue
        return results
    def __processInputElements(self):
        start_time = time.time()
        results = []
        elements = self.driver.find_elements(By.TAG_NAME, "input")
        for element in elements:
            try:
                if not element.is_displayed():
                    continue
                result = self.__process_element(element)
                if result:
                    result["type"] = "input"
                    results.append(result)
            except Exception as exp:
                traceback.print_exc()
        end_time = time.time()
        if len(results) > 20:
            results = results[:20]
        # print(f"__processInputElements time: {end_time - start_time:.2f} seconds")
        return results
    def processURL(self, url, save_name=None):
        if save_name is None:
            save_name = generate_url_hash(url)
        save_path = os.path.join(self.img_dir, save_name + ".png")
        save_mhtml_path = os.path.join(self.mhtml_dir, save_name + ".mhtml")
        if os.path.exists(save_path) and os.path.exists(save_mhtml_path):
            self.logger.info(f"Skipping URL: {url} - Files already exist")
            return None

        print("Begin save")
        self.accessURL(url)

        # 等待第一个div加载结束，如果超时则跳过此URL
        wait = WebDriverWait(self.driver, self.wait_timeout+12)  # 设置最长等待时间
        self.logger.info("waiting")
        try:
            wait.until(EC.presence_of_element_located((By.XPATH, "//div")))
        except TimeoutException:
            self.logger.warning(f"Timeout waiting for elements on URL: {url}. Skipping...")
            return None

        # 再sleep 5s
        time.sleep(5)

        self.saveMHTML(save_mhtml_path)
        # time.sleep(4)
        # width = self.width
        # height = self.height

        # results = self.__processClickableElements()
        # if self.scrape_hover:
        #     hovers = self.__processHoverElementsV2()
        #     results.extend(hovers)
        
        # self.saveScreenshot(save_path)
        # image = Image.open(save_path)
        # image = image.resize([width, height])

        # for result in results:
        #     result['url'] = url
        #     result['image_path'] = save_path
        #     self.logger.info(result)

        # if self.draw_box:
        #     draw = ImageDraw.Draw(image)
        #     for result in results:
        #         left_top = result['left-top']
        #         size = result['size']
        #         draw.rectangle([left_top, (left_top[0] + size[0], left_top[1] + size[1])], outline=self.box_color,
        #                     width=2)

        # image.save(save_path)
        # image.close()
        return results

    @staticmethod
    def isLeafElement(element):
        """判断元素是否是叶子节点"""
        # children = element.find_elements(By.XPATH, './*')
        html = element.get_attribute('innerHTML')
        # 使用正则表达式匹配包含子元素的HTML标签
        pattern = r'<[^>]*>.*?<[^>]*>'
        # 查找匹配项
        match = re.search(pattern, html, re.DOTALL)

        if match:
            return True
        else:
            return False


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser()
#     parser.add_argument("--test_url", type=str,
#                         default="http://rubemrobierbstudio.com/portfolio/love-changes-everything/")
#     parser.add_argument("--driver_path", type=str, default="/cpfs01/shared/public/web_crawler/chromedriver")
#     parser.add_argument("--img_dir", type=str, default="./images")
#     parser.add_argument("--mhtml_dir", type=str, default="./mhtmls")
#     parser.add_argument("--width", type=int, default=1920)
#     parser.add_argument("--height", type=int, default=1080)
#     parser.add_argument("--wait_timeout", type=int, default=6)
#     parser.add_argument("--draw_box", action="store_true")
#     parser.add_argument("--scrape_hover", action="store_true")
#     parser.add_argument("--nogui", action="store_true")
#     args = parser.parse_args()

#     logger = logging.getLogger(__name__)
#     crawler = Crawler(args.driver_path, args.img_dir, args.mhtml_dir, args.width, args.height, args.wait_timeout, logger,
#                       args.draw_box, args.scrape_hover, args.nogui)
#     crawler.processURL(args.test_url)
#     crawler.quit()
