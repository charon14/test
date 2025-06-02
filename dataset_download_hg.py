import pandas as pd
import urllib.request
from tqdm import tqdm
import requests

# file_url = "https://huggingface.co/datasets/OpenFace-CQUPT/FaceCaption-15M/resolve/main/FaceCaption-v2.parquet"
# file_path = ""

file_url = "https://huggingface.co/datasets/toloka/WSDMCup2023/resolve/main/data/train-00000-of-00001.csv"
file_path = "./download/toloka.csv"


def download_file(url, file_path,proxies=None):
    with requests.get(url, stream=True, proxies=proxies) as response:
        response.raise_for_status() #如果响应码是4xx或5xx，抛出异常
        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024
        with open(file_path, 'wb') as file, tqdm(
            desc=file_path,
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        ) as bar:
            for chunk in response.iter_content(chunk_size=block_size):
                if chunk:  # filter out keep-alive new chunks
                    file.write(chunk)
                    bar.update(len(chunk))


if __name__ == "__main__":
    download_file(file_url, file_path)
            

