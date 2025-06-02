"""
预处理，把数据集原始文件转化为jsonl.temp文件，并输出一个url.txt文件
"""
import time
import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm
import sys

BATCH_SIZE = 10000 #每次处理10000条数据
DATASET_NAME = "FaceCaption15M"
dataset_path = "Datasets/FaceCaption15M/download/FaceCaption.parquet"
urls_txt_path = ""
output_file_path = ""

urls = set()

def init():
    global DATASET_NAME, dataset_path, urls_txt_path, output_file_path
    if DATASET_NAME.strip() == "" or dataset_path.strip() == "":
        print("请先设置数据集名称和数据集路径")
        sys.exit()

    urls_txt_path = f"Datasets/{DATASET_NAME}/urls.txt"
    output_file_path = f"Datasets/{DATASET_NAME}/output.jsonl.temp"
    

#返回一个可迭代对象,

def transform_data(row) -> dict:
    """
    Transform a single row of data into the desired format.
    """
    conversation = [{
        "from": "human",
        "value": "<image>\ndescribe the image in detail"
    }, {
        "from": "gpt",
        "value": row.caption[0]
    }]
    return {
        "id": None,  # id在写入文件的时候统一写入
        "image": [u.strip() for u in row.url.split(", ") if u != "UNKNOWN"],
        "height": None,
        "width": None,
        "conversations": conversation
    }



if __name__ == "__main__":
    init()
    
    #读取parquet文件
    parquet_file = pq.ParquetFile(dataset_path)
    with tqdm(total=parquet_file.metadata.num_rows, desc="Processing rows") as pbar:
        for batch in parquet_file.iter_batches(batch_size=BATCH_SIZE, columns=["_id", 'url', 'caption']):
            df = batch.to_pandas()
            with open(output_file_path, 'w', encoding='utf-8') as jsonl_file:
                for row in df.itertuples():
                    transformed_row = transform_data(row)
                    jsonl_file.write(f"{transformed_row}\n")
                    for u in transformed_row["image"]:
                        urls.add(u)
                    pbar.update(1)
    
    #写入urls.txt
    with open(urls_txt_path, 'w', encoding='utf-8') as f:
        for url in urls:
            f.write(f"{url}\n")
                

                