import asyncio
import signal
import binascii
import os

import aiohttp
import bencode2 as bencoder
from maga.crawler import Maga
from maga.downloader import get_metadata

# API端点，用于添加新任务
API_URL = "http://47.79.229.105:8000/tasks/"

# 使用一个集合（set）来记录已经处理过的infohash，防止重复下载
PROCESSED_INFOHASHES = set()
# 确保保存.torrent文件的目录存在
os.makedirs("torrents", exist_ok=True)


def format_bytes(size):
    """将字节大小格式化为可读的字符串（KB, MB, GB等）"""
    if size is None:
        return "N/A"
    power = 1024
    n = 0
    power_labels = {0: '', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power and n < len(power_labels) - 1:
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"


def contains_mp4(info):
    """
    检查种子元数据(info字典)中是否包含.mp4文件。
    支持单文件和多文件种子。
    """
    # 检查多文件种子
    if b'files' in info and info[b'files']:
        for file_info in info[b'files']:
            # file_info[b'path'] 是一个路径段列表，最后一个是文件名
            if file_info[b'path']:
                try:
                    # 获取文件名并解码，忽略解码错误，转为小写进行不区分大小写的比较
                    filename = file_info[b'path'][-1].decode(errors='ignore').lower()
                    if filename.endswith('.mp4'):
                        return True
                except Exception:
                    # 如果解码或其他操作失败，则跳过此文件
                    continue
    # 检查单文件种子
    elif b'name' in info:
        try:
            # 获取文件名并解码，忽略解码错误，转为小写
            filename = info[b'name'].decode(errors='ignore').lower()
            if filename.endswith('.mp4'):
                return True
        except Exception:
            # 如果解码失败，则认为不包含
            return False

    # 如果遍历完所有文件都未找到.mp4，则返回False
    return False


async def add_task_to_downloader(infohash_hex, torrent_file_path):
    """
    异步函数，用于将infohash和种子文件上传到下载器以添加新任务
    """
    try:
        async with aiohttp.ClientSession() as session:
            # 构建 multipart/form-data 请求体
            data = aiohttp.FormData()
            data.add_field('infohash', infohash_hex)
            
            # 打开并添加种子文件到请求体
            # aiohttp 会自动处理文件的异步读取
            with open(torrent_file_path, 'rb') as torrent_file:
                data.add_field('torrent_file',
                               torrent_file,
                               filename=os.path.basename(torrent_file_path),
                               content_type='application/x-bittorrent')
            
                # 发送POST请求
                async with session.post(API_URL, data=data) as response:
                    if response.status == 200 or response.status == 201:
                        print(f"  [API] 成功上传并添加任务: {infohash_hex}")
                    else:
                        # 打印带有状态码和响应内容的错误信息
                        response_text = await response.text()
                        print(f"  [API] 添加任务失败: {infohash_hex}, "
                              f"状态码: {response.status}, 响应: {response_text}")

    except aiohttp.ClientError as e:
        # 捕获网络连接相关的错误
        print(f"  [API] 请求失败: {infohash_hex} -> {e}")
    except FileNotFoundError:
        print(f"  [API] 找不到要上传的种子文件: {torrent_file_path}")
    except Exception as e:
        # 捕获其他未知异常
        print(f"  [API] 发生未知错误: {infohash_hex} -> {e}")


async def main():
    loop = asyncio.get_running_loop()

    # 定义当爬虫发现新infohash时的回调函数
    async def on_infohash_discovered(infohash, peer_addr):
        infohash_hex = binascii.hexlify(infohash).decode()

        # 如果这个infohash还没有被处理过
        if infohash_hex not in PROCESSED_INFOHASHES:
            PROCESSED_INFOHASHES.add(infohash_hex)

            # 直接调用下载器函数，传入infohash和peer地址
            # get_metadata返回的直接就是info字典
            info = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)

            # 只有在成功获取到元数据(info字典)时才打印信息
            if info:
                # 为了生成一个有效的.torrent文件，我们需要一个顶层的字典
                torrent_dict = {b'info': info}

                # 提取核心信息用于打印
                name = info.get(b'name', b'Unknown').decode(errors='ignore')
                if b'files' in info:
                    num_files = len(info[b'files'])
                    total_size = sum(f[b'length'] for f in info[b'files'])
                else:
                    num_files = 1
                    total_size = info.get(b'length')

                # 将元数据保存到.torrent文件
                file_path = os.path.join("torrents", f"{infohash_hex}.torrent")
                try:
                    with open(file_path, "wb") as f:
                        # 我们需要对包含info字典的顶层字典进行bencode编码
                        f.write(bencoder.bencode(torrent_dict))

                    # 打印摘要
                    print("=" * 30 + " 下载成功 " + "=" * 30)
                    print(f"  Infohash: {infohash_hex}")
                    print(f"  文件名: {name}")
                    print(f"  文件数: {num_files}")
                    print(f"  总大小: {format_bytes(total_size)}")
                    print(f"  已保存到: {file_path}")

                    # ======================================================
                    # 检查是否包含 .mp4 文件, 如果包含则提交任务
                    # ======================================================
                    if contains_mp4(info):
                        print(f"  [检查] 元数据中发现 .mp4 文件, 准备提交任务。")
                        # 传入infohash和文件路径
                        await add_task_to_downloader(infohash_hex, file_path)
                    else:
                        print(f"  [检查] 元数据中未发现 .mp4 文件, 跳过任务提交。")

                    print("=" * 70 + "\n")

                except Exception as e:
                    # 仅在保存失败时打印错误
                    print(f"[保存失败] {infohash_hex} -> {e}")

    # 创建并运行爬虫
    crawler = Maga(loop=loop, handler=on_infohash_discovered)
    await crawler.run(port=6882)

    print("服务已启动，正在后台监听和下载...")
    print("只有成功下载的种子才会被打印出来。")
    print("按 Ctrl+C 停止运行。")

    # 等待程序被中断
    stop = asyncio.Future()
    loop.add_signal_handler(signal.SIGINT, stop.set_result, None)
    await stop

    # 优雅地关闭服务
    print("\n正在停止服务...")
    crawler.stop()
    print("服务已停止。")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
