"""
从../Datasets/{dataset_name}/urls.txt中读取下载链接，使用异步方式下载图片到../Datasets/{dataset_name}/media/目录下。
"""
import asyncio
import json
import time
import aiofiles
import aiohttp
import pyarrow.parquet as pq
import hashlib
import os
from tqdm.asyncio import tqdm
import sys
import magic

urls_file = ""  # urls.txt的路径
download_failed_path = ""  # 下载失败的链接文件路径
unknowntype_path= ""  # 无法识别文件类型的链接文件路径
media_path_map = ""
DATASET_NAME = ""  # 数据集名称
SAVE_DIR = ""  # 保存图片的目录


"""===========媒体文件下载相关参数============"""
CONCURRENT_DOWNLOADS = 50
MIME_EXTENSION_MAP = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/webp": "webp",
    "image/bmp": "bmp",
    # 如果还有其它常见格式，可以继续加
}
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122 Safari/537.36"
}
proxy = None  # 代理设置，如果需要的话可以在这里配置
error_link = [] #记录下载失败的链接
unknowntype_file = []  #记录无法识别文件类型的链接
path_map = {}  # {url: file_name} 映射关系
path_map_lock = asyncio.Lock()  # 用于保护 path_map 的锁
error_lock = asyncio.Lock()
unknowntype_file_lock = asyncio.Lock()
"""======================="""


def init(t):
    global media_path_map, urls_file, SAVE_DIR, DATASET_NAME, download_failed_path, unknowntype_path
    DATASET_NAME = t.strip()
    urls_file = f"../Datasets/{DATASET_NAME}/urls.txt"
    SAVE_DIR = f"../Datasets/{DATASET_NAME}/media/"
    download_failed_path = f"../Datasets/{DATASET_NAME}/download_failed.txt"
    unknowntype_path = f"../Datasets/{DATASET_NAME}/unknown_type.txt"
    media_path_map = f"../Datasets/{DATASET_NAME}/media_path_map.json"
    if not os.path.exists(urls_file):
        print(f"文件 {urls_file} 不存在，请检查路径是否正确。")
        sys.exit()
    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

def get_urls():
    urls = []
    with open(urls_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                urls.append(line.strip())
    return urls

async def download_image(session:aiohttp.ClientSession, url:str, sem:asyncio.Semaphore):
    """
    下载单张图片，将其按照 URL 的 MD5 哈希值命名。
    遇到失败时，将 (url, 错误信息) 添加到 error_link 中。
    """
    # 先计算哈希，用作临时文件无后缀的基础名
    hash_digest = hashlib.md5(url.encode('utf-8')).hexdigest()
    tmp_filename = os.path.join(SAVE_DIR, f"{hash_digest}")  # 后缀下载文件判断类型后追加
    try:
        async with sem:
            async with session.get(url) as response:
                #非200的响应直接记录错误
                if response.status != 200:
                    async with error_lock:
                        error_link.append([url, f"HTTP Error: {response.status}"])
                    return
                # 如果状态码是 200，则读取完整图片数据
                data: bytes = await response.read()
                
                # 先尝试从 Response Header 里获取 Content-Type
                content_type = response.headers.get("Content-Type", "").split(";")[0].strip().lower()
                ext = None
                if content_type in MIME_EXTENSION_MAP:
                    ext = MIME_EXTENSION_MAP[content_type]
                else:
                    # Header 中无法识别或者拿不到，就使用 python-magic 从内容检测 MIME
                    try:
                        mime_detected = magic.from_buffer(data, mime=True)
                        ext = MIME_EXTENSION_MAP.get(mime_detected, None)
                    except Exception as e:
                        # magic 检测失败，则记录但继续尝试保存为 bin
                        ext = None
                # 如果最后依然没有获得已知后缀，就统一使用 .bin
                if not ext:
                    ext = "bin"
                    with unknowntype_file_lock:
                        unknowntype_file.append(url)
                
                # 构造最终文件名，并保存
                filename_with_ext = f"{tmp_filename}.{ext}"
                async with aiofiles.open(filename_with_ext, "wb") as f:
                    await f.write(data)
                # 更新 path_map
                async with path_map_lock:
                    path_map[url] = filename_with_ext
                    
    except asyncio.TimeoutError:
        # 超时异常
        async with error_lock:
            error_link.append([url, "TimeoutError"])
    except aiohttp.ClientError as e:
        # aiohttp 相关异常（例如连接被拒绝、DNS 失败等）
        async with error_lock:
            error_link.append([url, f"ClientError: {str(e)}"])
    except Exception as e:
        # 其他未预见的异常
        async with error_lock:
            error_link.append([url, f"UnexpectedError: {str(e)}"])

async def async_download_main(urls):
    sem = asyncio.Semaphore(CONCURRENT_DOWNLOADS)
    timeout = aiohttp.ClientTimeout(total=15, connect=10, sock_read=10) #超时参数
    connector = aiohttp.TCPConnector(ssl=False)  # 关闭 SSL 验证
    async with aiohttp.ClientSession(timeout=timeout,connector=connector,proxy=None,headers=headers) as session:
        tasks = [
            download_image(session, url, sem)
            for url in urls
        ]
        await tqdm.gather(*tasks)



if __name__ == "__main__":
    init(input("请输入数据集名称: "))
    urls = get_urls()
    
    # 异步下载
    start_time = time.time()
    asyncio.run(async_download_main(urls=urls))
    end_time = time.time()
    print(f"Downloaded {len(urls)} images in {end_time - start_time:.2f} seconds")


    #写入download_failed.txt
    with open(download_failed_path, 'w', encoding='utf-8') as f:
        for url, error in error_link:
            f.write(f"{url}\t{error}\n")
    #写入unknown_type.txt
    with open(unknowntype_path, 'w', encoding='utf-8') as f:
        for url in unknowntype_file:
            f.write(f"{url}\n")
    #写入media_path_map.json
    with open(media_path_map, 'w', encoding='utf-8') as f:
        json.dump(path_map, f, ensure_ascii=False, indent=4)

    #打印统计信息
    print(f"len(urls): {len(urls)}")
    print(f"len(error_link): {len(error_link)}")
    print(f'image_count: {len(os.listdir(SAVE_DIR))}')



        
    
