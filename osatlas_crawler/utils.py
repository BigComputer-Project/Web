from tqdm import tqdm
import hashlib

import json
def extract_urls_from_cdx(cdx_file_path):
    urls = []
    with open(cdx_file_path, 'r', encoding='utf-8') as file:
        for line in tqdm(file):
            line = line.strip().rstrip(',').strip()
            if line:
                try:
                    # Parse line as a JSON list
                    url_list = json.loads(line)
                    if isinstance(url_list, list):
                        urls.extend(url_list)  # Append each URL in the list to the main URLs list
                    else:
                        print(f"Skipping non-list entry: {line}")
                except json.JSONDecodeError:
                    print(f"Skipping malformed JSON line: {line}")
    return urls


# 定义一个函数来生成URL的哈希码
def generate_url_hash(url):
    md5 = hashlib.md5()
    md5.update(url.encode('utf-8'))
    return md5.hexdigest()
