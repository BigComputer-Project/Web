import logging

from crawel import Crawler
import os
import multiprocessing
import random
import json
import argparse
from utils import extract_urls_from_cdx
import psutil
import time
def configLogging(loglevel):
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
    logging.basicConfig(level=level, format="%(asctime)s [%(process)d] %(message)s")


def split_task_files(cdx_file_path, out_dir, num_workers, url_st, num_urls):
    urls = extract_urls_from_cdx(cdx_file_path)
    random.shuffle(urls)
    urls = urls[url_st:url_st + num_urls]  # 处理前1w个
    num_urls = len(urls)
    num_urls_per_worker = num_urls // num_workers
    for i in range(num_workers):
        start_index = i * num_urls_per_worker
        end_index = (i + 1) * num_urls_per_worker
        if i == num_workers - 1:
            end_index = num_urls
        with open(os.path.join(out_dir, f"{i}.txt"), 'a', encoding='utf-8') as file:
            file.write('\n')
            for j in range(start_index, end_index):
                file.write(urls[j] + '\n')


# 定义一个函数，用于并行执行的任务
def worker_function(args):
    worker_num = args[0]
    in_dir = args[1]
    width = args[2]
    height = args[3]
    wait_timeout = args[4]
    scrape_hover = args[5]
    loglevel = args[6]
    in_file = open(os.path.join(in_dir, f"{worker_num}.txt"), 'r', encoding='utf-8')
    out_file = open(os.path.join(in_dir, f"{worker_num}_out.txt"), 'a', encoding='utf-8')
    out_image_dir = os.path.join(in_dir, f"{worker_num}_images")
    out_mhtml_dir = os.path.join(in_dir, f"{worker_num}_mhtmls")
    driver_path = '/cpfs01/shared/public/wangyian/code/SeeClick/seeclick-crawler/seeclick-crawler/chromedriver-linux64/chromedriver'
    configLogging(loglevel)
    logger = logging.getLogger(f"worker_{worker_num}")
    crawler = Crawler(driver_path, out_image_dir, out_mhtml_dir, width, height, wait_timeout, logger, draw_box=False,
                      scrape_hover=scrape_hover,
                      nogui=True)
    for i, url in enumerate(in_file):
        try:
            results = crawler.processURL(url.strip())
        except Exception as exp:
            logger.error(f"Worker {worker_num} encountered an exception when processing {url}: {exp}")
            continue
        # if results:
        #     for result in results:
        #         out_file.write(json.dumps(result) + '\n')
        # out_file.flush()

        if i > 0 and i % 100 == 0:
            logger.info(f"Worker {worker_num} has processed {i} urls")
            crawler.restart()  # 重新启动一下browser，防止内存泄漏
            logger.info(f"Worker {worker_num} has restarted the browser")
    crawler.quit()

def monitor_processes():
    while True:
        for proc in psutil.process_iter(['name', 'memory_percent']):
            if proc.info['name'] == 'node' and proc.info['memory_percent'] > 8:  # 如果node进程占用超过5%的内存
                proc.terminate()  # 尝试终止进程
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cdx_file_path", type=str, default='/cpfs01/shared/public/wangyian/data/web_crawl/cleaned_urls_1.json', help='path to url json, please save as a list of urls')
    parser.add_argument("--out_root", type=str, default='./data/tasks_ext_3', help='dir to store tasks(.mhtml)')
    parser.add_argument("--worker_id", type=int, default=0)
    parser.add_argument("--num_workers", type=int, default=10, help='number of workers')
    parser.add_argument("--driver_path", type=str, default='/nas/shared/NLP_A100/wangyian/chromedriver-linux64/chromedriver', help='your chromedriver path')
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num_urls", type=int, default=200000)
    parser.add_argument("--width", type=int, default=1920)
    parser.add_argument("--height", type=int, default=1080)
    parser.add_argument("--wait_timeout", type=int, default=20)
    parser.add_argument("--scrape_hover", action='store_true')
    parser.add_argument("--loglevel", type=str, default='INFO')

    args = parser.parse_args()
    # 创建一个进程池，指定最大进程数
    num_workers = args.num_workers
    monitor_process = multiprocessing.Process(target=monitor_processes)
    monitor_process.start()
    pool = multiprocessing.Pool(processes=num_workers)

    # 使用进程池并行执行任务
    cdx_file_path = args.cdx_file_path
    out_dir = os.path.join(args.out_root, f'tasks{args.worker_id}')
    url_st = args.worker_id * args.num_urls
    os.makedirs(out_dir, exist_ok=True)
    random.seed(args.seed)
    split_task_files(cdx_file_path, out_dir, num_workers, url_st, args.num_urls)
    args_list = [(i, out_dir, args.width, args.height, args.wait_timeout, args.scrape_hover, args.loglevel) for i in
                 range(num_workers)]
    pool.map(worker_function, args_list)
    # 关闭进程池，等待所有进程完成
    pool.close()
    pool.join()
    monitor_process.terminate()
    monitor_process.join()
    print("All workers have finished")
