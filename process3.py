"""
在媒体文件已经下载到本地，有初步jsonl文件的情况下，进一步处理jsonl文件
"""
import json
import os
import aiofiles
import asyncio
import hashlib
from tqdm import tqdm
import sys
from PIL import Image
input_jsonl_file = ""
media_path_map_file= ""
output_jsonl_file = ""
DATASET_NAME = ""

media_path_map = {}  #{url:file_name}
#返回一个jsonl文件的迭代器
def init(t):
    global input_jsonl_file, output_jsonl_file, media_path_map_file, DATASET_NAME, media_path_map
    DATASET_NAME = t.strip()
    input_jsonl_file = f"../Datasets/{DATASET_NAME}/output.jsonl.temp"
    output_jsonl_file = f"../Datasets/{DATASET_NAME}/output.jsonl"
    media_path_map_file = f"../Datasets/{DATASET_NAME}/media_path_map.json"
    if not os.path.exists(input_jsonl_file):
        print(f"文件 {input_jsonl_file} 不存在，请检查路径是否正确。")
        sys.exit()
    
    #可选 检查媒体文件夹下是否存在文件
    media_dir = f"../Datasets/{DATASET_NAME}/media/"
    if len(os.listdir(media_dir)) == 0:
        print(f"媒体文件夹 {media_dir} 为空，请先下载媒体文件。")
        sys.exit()
    if not os.path.exists(media_path_map_file):
        print(f"媒体路径映射文件 {media_path_map_file} 不存在，请先下载媒体文件。")
        sys.exit()
    with open(media_path_map_file,'r',encoding="utf-8") as f:
        media_path_map = json.load(f)
    

    #处理单行数据
def process_row_data(row_data):
    """
    处理每一行数据，将图片链接转换为本地文件路径，并添加必要的字段。
    """
    processed_row = row_data.copy()
    urls = row_data["image"]
    for u in urls:
        if u in media_path_map:
            media_file_name = media_path_map[u]
            media_file_path = f"media/{media_file_name}"
            processed_row["image"] = media_file_path
            w,h = Image.open(media_file_path).size
            processed_row["width"] = w
            processed_row["width"] = h
            return processed_row
    return {}


if __name__ == "__main__":
    init(input("请输入数据集名称: "))
    total_rows = sum(1 for _ in open(input_jsonl_file, 'r', encoding='utf-8'))  # 计算总行
    
    #逐行处理文件
    with open(input_jsonl_file, 'r', encoding='utf-8') as input_file, \
         open(output_jsonl_file, 'w', encoding='utf-8') as output_file, \
            tqdm(desc="Processing rows", unit="row",total=total_rows) as pbar:
        for row in input_file:
            row_data = json.loads(row.strip())
            processed_row = process_row_data(row_data)
            if processed_row:
                output_file.write(json.dumps(processed_row, ensure_ascii=False) + '\n')
            pbar.update(1)
    
    #输出信息


