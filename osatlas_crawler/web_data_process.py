from itertools import chain
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from PIL import Image, ImageDraw, ImageFont
import json
import os
from selenium.common.exceptions import TimeoutException
import traceback
import time
import io
import multiprocessing
from functools import partial
import math
from tqdm import tqdm
import argparse
def save_results(results, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
def create_results_directory():
    if not os.path.exists('results'):
        os.makedirs('results')

def save_results(results, output_file):
    with open(os.path.join('results', output_file), 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

class WebPageAnalyzer:
    def __init__(self, mhtml_path, driver, width=1920, height=1080):
        self.mhtml_path = mhtml_path
        self.driver = driver
        self.width = width
        self.height = height
        self.wait = WebDriverWait(self.driver, 10)
        self.load_page()

    def load_page(self):
        print("Loading MHTML file...")
        start_time = time.time()
        self.driver.get(f"file://{os.path.abspath(self.mhtml_path)}")
        try:
            WebDriverWait(self.driver, 300).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        except TimeoutException:
            print(f"Timed out loading {self.mhtml_path}")
        self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        self.driver.set_window_size(self.width, self.height)
        
        self.width = self.driver.execute_script("return document.body.scrollWidth")
        self.height = self.driver.execute_script("return document.body.scrollHeight")
        end_time = time.time()
        print(f"MHTML file loaded in {end_time - start_time:.2f} seconds")
    
    def findAllHiddenElements(self):
        start_time = time.time()
        elements = self.driver.find_elements(By.XPATH, "//*[contains(@style,'display:none') or contains(@style,'visibility:hidden')]")
        end_time = time.time()
        # print(f"findAllHiddenElements time: {end_time - start_time:.2f} seconds")
        return elements

    def findAllNotHiddenElements(self):
        start_time = time.time()
        elements = self.driver.find_elements(By.XPATH, "//*")
        not_hidden = [element for element in elements if element.is_displayed()]
        end_time = time.time()
        # print(f"findAllNotHiddenElements time: {end_time - start_time:.2f} seconds")
        return not_hidden

    def findAllClickableElements(self):
        start_time = time.time()
        elements = self.driver.find_elements(By.XPATH, "//a | //button | //input[@type='submit'] | //*[@onclick]")
        end_time = time.time()
        # print(f"findAllClickableElements time: {end_time - start_time:.2f} seconds")
        if len(elements) > 40:
            elements = elements[:40]
        return elements

    def findAllTitledElements(self):
        start_time = time.time()
        elements = self.driver.find_elements(By.XPATH, "//*[@title]")
        end_time = time.time()
        if len(elements) > 20:
            elements = elements[:20]
        # print(f"findAllTitledElements time: {end_time - start_time:.2f} seconds")
        return elements
    
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

    def __processSVGElements(self):
        start_time = time.time()
        results = []
        elements = self.driver.find_elements(By.TAG_NAME, "svg")
        for element in elements:
            try:
                if not element.is_displayed():
                    continue
                result = self.__process_element(element)
                if result:
                    result["type"] = "svg"
                    results.append(result)
            except Exception as exp:
                traceback.print_exc()
        end_time = time.time()
        # print(f"__processSVGElements time: {end_time - start_time:.2f} seconds")
        if len(results) > 20:
            results = results[:20]
        return results

    def __processScrollBars(self):
        start_time = time.time()
        results = []
        elements = self.driver.find_elements(By.XPATH, "//*[contains(@class, 'scroll') or contains(@class, 'scrollbar')]")
        for element in elements:
            try:
                if not element.is_displayed():
                    continue
                result = self.__process_element(element)
                if result:
                    result["type"] = "scrollbar"
                    results.append(result)
            except Exception as exp:
                traceback.print_exc()
        end_time = time.time()
        # print(f"__processScrollBars time: {end_time - start_time:.2f} seconds")
        return results

    def isLeafElement(self, element):
        return len(element.find_elements(By.XPATH, ".//*")) == 0

    def __processClickableElements(self):
        start_time = time.time()
        results = []
        signatures = set()
        elements = self.findAllClickableElements()
        for element in elements:
            try:
                left_top = (element.location['x'], element.location['y'])
                
                width, height = element.size['width'], element.size['height']
                right_bottom = (left_top[0] + width, left_top[1] + height)
                text = element.text
                if text is None or text == '':
                    text = element.get_attribute("value")
                if text is None or text == '' or width == 0 or height == 0:
                    continue
                if right_bottom[0] >= self.width or right_bottom[1] >= self.height:
                    continue
                signature = f'{left_top[0]}-{left_top[1]}-{width}-{height}'
                if signature in signatures:
                    continue
                signatures.add(signature)
                results.append({"left-top": left_top, "size": (width, height), "text": text, "type": "text"})
            except Exception as exp:
                traceback.print_exc()
                continue
        end_time = time.time()
        # print(f"__processClickableElements time: {end_time - start_time:.2f} seconds")
        return results

    def __processHoverElementsV2(self):
        start_time = time.time()
        results = []
        signatures = set()
        elements = self.findAllTitledElements()
        for element in elements:
            try:
                if not element.is_displayed():
                    continue
                left_top = (element.location['x'], element.location['y'])
                width, height = element.size['width'], element.size['height']
                right_bottom = (left_top[0] + width, left_top[1] + height)
                if width == 0 or height == 0 or right_bottom[0] >= self.width or right_bottom[1] >= self.height:
                    continue
                title = element.get_attribute("title")
                if title is None or title == '':
                    continue
                signature = f'{left_top[0]}-{left_top[1]}-{width}-{height}'
                if signature in signatures:
                    continue
                signatures.add(signature)
                results.append({"left-top": left_top, "size": (width, height), "text": title, "type": "hover"})
            except Exception as exp:
                traceback.print_exc()
                continue
        end_time = time.time()
        # print(f"__processHoverElementsV2 time: {end_time - start_time:.2f} seconds")
        return results

    def close(self):
        if self.driver:
            self.driver.quit()

    def capture_and_analyze_sections(self, output_dir, raw):
        num_sections = math.ceil(self.height / 1080)
        results = []
        k = 0
        for i in range(num_sections):
            scroll_to = min(i * 1080, self.height - 1080)  
            self.driver.execute_script(f"window.scrollTo(0, {scroll_to});")
            time.sleep(1.5) 
            actual_scroll = self.driver.execute_script("return window.pageYOffset;")

            
            screenshot = self.driver.get_screenshot_as_png()
            original_screenshot = Image.open(io.BytesIO(screenshot))

            
            annotated_screenshot = original_screenshot.copy()
            draw = ImageDraw.Draw(annotated_screenshot)
            flat_list = list(chain(*raw.values()))
            
            if flat_list != []:
                section_elements = self._process_elements_bbox(draw, 0, actual_scroll, flat_list)
                if section_elements != []:
                    original_screenshot.save(f'{output_dir}/section_{k+1}_original.png')
                    
                    annotated_screenshot.save(f'{output_dir}/section_{k+1}_annotated.png')
                    
                
                    json_filename = f'{output_dir}/section_{k+1}_elements.json'
                    with open(json_filename, 'w') as json_file:
                        json.dump(section_elements, json_file, indent=4)
                    
                    k += 1

            if actual_scroll + 1080 >= self.height:
                break

    def _process_elements_bbox(self, draw, x_offset, y_offset, elements):
        section_elements = []
        for element in elements:
            left, top = element['left-top']
            width, height = element['size']
            
            right = left + width
            bottom = top + height
            
            
            adjusted_x1 = max(0, min(left - x_offset, 1920))
            adjusted_y1 = max(0, min(top - y_offset, 1080))
            adjusted_x2 = max(0, min(right - x_offset, 1920))
            adjusted_y2 = max(0, min(bottom - y_offset, 1080))
            
            
            if adjusted_y2 > 0 and adjusted_y1 < 1080:
                draw.rectangle([adjusted_x1, adjusted_y1, adjusted_x2, adjusted_y2], outline="red")
                
            
            
                section_element = {
                    "bbox": [adjusted_x1, adjusted_y1, adjusted_x2, adjusted_y2],
                    "type": element['type'],
                    "text": element['text'],
                    "original_position": {
                        "left-top": element['left-top'],
                        "size": element['size']
                    }
                    }
                if (section_element['bbox'] != []) and (section_element['type'] is not None) and (section_element['text'] is not None):
                    section_elements.append(section_element)
        
        return section_elements
    

    def __processSearchBars(self):
        start_time = time.time()
        results = []
        elements = self.driver.find_elements(By.XPATH, "//input[@type='search' or contains(@class, 'search')]")
        for element in elements:
            try:
                if not element.is_displayed():
                    continue
                result = self.__process_element(element)
                if result:
                    result["type"] = "searchbar"
                    results.append(result)
            except Exception as exp:
                traceback.print_exc()
        end_time = time.time()
        # print(f"__processSearchBars time: {end_time - start_time:.2f} seconds")
        return results
    def findAllAltElements(self):
        """Finds elements with the 'alt' attribute."""
        elements = self.driver.find_elements(By.XPATH, "//*[@alt]")
        return elements

    def findAllAriaLabelElements(self):
        """Finds elements with the 'aria-label' attribute."""
        elements = self.driver.find_elements(By.XPATH, "//*[@aria-label]")
        return elements

    def __processAltElements(self):
        results = []
        elements = self.findAllAltElements()
        for element in elements:
            try:
                if not element.is_displayed():
                    continue
                result = self.__process_element(element)
                if result:
                    result["type"] = "alt"
                    result["attribute"] = "alt"
                    results.append(result)
            except Exception:
                traceback.print_exc()
        return results

    def __processAriaLabelElements(self):
        results = []
        elements = self.findAllAriaLabelElements()
        for element in elements:
            try:
                if not element.is_displayed():
                    continue
                result = self.__process_element(element)
                if result:
                    result["type"] = "aria-label"
                    result["attribute"] = "aria-label"
                    results.append(result)
            except Exception:
                traceback.print_exc()
        return results
    def __process_element(self, element):
        left_top = (element.location['x'], element.location['y'])
        width, height = element.size['width'], element.size['height']
        right_bottom = (left_top[0] + width, left_top[1] + height)
        if width == 0 or height == 0 or right_bottom[0] >= self.width or right_bottom[1] >= self.height:
            return None
        return {
            "left-top": left_top,
            "size": (width, height),
            "text": element.text or element.get_attribute("value") or ""
        }
        
    def analyze(self):
        if not os.path.exists('results'):
            os.makedirs('results')
        
        results = {
            "clickable_elements": self.__processClickableElements(),
            "hover_elements": self.__processHoverElementsV2(),
            "input_elements": self.__processInputElements(),
            "svg_elements": self.__processSVGElements(),
            "scroll_bars": self.__processScrollBars(),
            "search_bars": self.__processSearchBars(),
            "alt": self.__processAriaLabelElements(),
            "aria_label": self.__processAriaLabelElements()
        }
        return results

    def _draw_element(self, draw, element, offset_x, offset_y):
        left, top = element["left-top"]
        width, height = element["size"]
        adjusted_left = left - offset_x
        adjusted_top = top - offset_y

        if 0 <= adjusted_top < 1080 and adjusted_top + height > 0:
            if element.get("type") == "svg":
                color = "yellow"
            elif element.get("type") == "input":
                color = "green"
            else:
                color = "red"

            draw.rectangle([adjusted_left, adjusted_top, adjusted_left + width, adjusted_top + height], 
                        outline=color, width=2)
            
            text = element.get("text", "")
            
            # draw.text((adjusted_left, adjusted_top - 10), text[:20], fill=color)  



def process_mhtml_files(root_dir, driver_path):
    for subdir, dirs, files in os.walk(root_dir):
        if subdir.endswith('_mhtml'):
            output_dir = subdir.replace('_mhtml', '_data')
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            for file in files:
                if file.endswith('.mhtml'):
                    mhtml_path = os.path.join(subdir, file)
                    output_subdir = os.path.join(output_dir, file[:-6])  # 移除 .mhtml 后缀
                    if not os.path.exists(output_subdir):
                        os.makedirs(output_subdir)
                    
                    process_single_mhtml(mhtml_path, output_subdir, driver_path)

def save_progress(root_dir, last_processed_file):
    progress_file = os.path.join(root_dir, 'progress.json')
    with open(progress_file, 'w') as f:
        json.dump({'last_processed': last_processed_file}, f)

def load_progress(root_dir):
    progress_file = os.path.join(root_dir, 'progress.json')
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            return json.load(f)['last_processed']
    return None

def process_single_mhtml(mhtml_path, output_dir, driver, root_dir,hash_data):
    # Check if this file has already been processed
    # if os.path.exists(os.path.join(output_dir, 'elements.json')):
    #     print(f"Skipping {mhtml_path} as it has already been processed.")
    #     return

    
    try:
        # Analyze the page and save results
        analyzer = WebPageAnalyzer(mhtml_path, driver)
        results = analyzer.analyze()
        with open(os.path.join(output_dir, 'elements.json'), 'w') as f:
            json.dump(results, f, indent=4)

        # Capture and analyze multiple sections
        analyzer.capture_and_analyze_sections(output_dir, results)
        
        mhtml_filename = os.path.basename(mhtml_path)
        key = os.path.splitext(mhtml_filename)[0] 
        # print("begin")
        # st = time.time()
        url = next((item[key] for item in hash_data if key in item), None)
        # dt = time.time()
        # print(dt-st)
        
        # if url is None:
        #     print(f"Warning: URL not found for key {key}")
        
        
        
        task_number = os.path.basename(os.path.dirname(output_dir))
        image_path = f"{root_dir}/{task_number}/{task_number}_image/{key}.png"
        
        
        json_data = {
            "url": url,
            "image_path": image_path
        }
        
        json_filename = f"{key}.json"
        json_filepath = os.path.join(output_dir, json_filename)
        
        with open(json_filepath, 'w') as f:
            json.dump(json_data, f, indent=4)

        # print(f"JSON file created: {json_filepath}")
    except Exception as e:
        print(f"Error processing {mhtml_path}: {str(e)}")

def setup_driver(driver_path, width=1920, height=1080):
    service = Service(executable_path=driver_path)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument(f"--window-size={width},{height}")
    chrome_options.add_argument('--disable-logging')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    print("Setting up driver...")
    start_time = time.time()
    driver = webdriver.Chrome(service=service, options=chrome_options)
    end_time = time.time()
    print(f"Driver setup time: {end_time - start_time:.2f} seconds")
    return driver

def process_subdirectory(subdir, driver_path, hash_data, root_dir):
    driver = setup_driver(driver_path)
    try:
        output_dir = subdir.replace('_mhtml', '_sampled_data')
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        for file in os.listdir(subdir):
            if file.endswith('.mhtml'):
                mhtml_path = os.path.join(subdir, file)
                output_subdir = os.path.join(output_dir, file[:-6])  
                if not os.path.exists(output_subdir):
                    os.makedirs(output_subdir)
                
                process_single_mhtml(mhtml_path, output_subdir, driver, root_dir, hash_data)
        driver.quit()
    finally:
        driver.quit()

def process_mhtml_files_with_resume(root_dir, driver_path, hash_data):
    subdirs = [os.path.join(root_dir, d) for d in os.listdir(root_dir) if d.endswith('_mhtmls')]
    
    # if subdirs == []:
    #     subdirs = [os.path.join(root_dir, d) for d in os.listdir(root_dir) if d.endswith('_mhtml')]
    process_func = partial(process_subdirectory, driver_path=driver_path, hash_data=hash_data, root_dir=root_dir)
    
   
    num_processes = multiprocessing.cpu_count()
    
    # Create a pool of worker processes
    with multiprocessing.Pool(processes=num_processes) as pool:
        # Use tqdm to show progress
        list(tqdm(pool.imap(process_func, subdirs), total=len(subdirs)))



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Process MHTML files with URL mapping.")
    parser.add_argument('--root_dir', type=str, required=True, help="Root directory for MHTML files.")
    parser.add_argument('--driver_path', type=str, required=True, help="Path to the Chrome WebDriver executable.")
    parser.add_argument('--hash_path', type=str, required=True, help="Path to the JSON file containing URL hash mappings.")

    args = parser.parse_args()
    
    # Assign the arguments to variables
    root_dir = args.root_dir
    driver_path = args.driver_path
    hash_path = args.hash_path

    # Load hash data
    with open(hash_path, 'r') as f:
        hash_data = [json.loads(line) for line in f]
    
    # Process MHTML files with the parsed arguments
    process_mhtml_files_with_resume(root_dir, driver_path, hash_data)