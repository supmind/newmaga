import asyncio
import signal
import binascii
import os

import bencode2 as bencoder
from maga.crawler import Maga
from maga.downloader import get_metadata

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
    while size > power and n < len(power_labels) -1 :
        size /= power
        n += 1
    return f"{size:.2f} {power_labels[n]}B"


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
                    print("="*30 + " 下载成功 " + "="*30)
                    print(f"  Infohash: {infohash_hex}")
                    print(f"  文件名: {name}")
                    print(f"  文件数: {num_files}")
                    print(f"  总大小: {format_bytes(total_size)}")
                    print(f"  已保存到: {file_path}")
                    print("="*70 + "\n")

                except Exception as e:
                    # 仅在保存失败时打印错误
                    print(f"[保存失败] {infohash_hex} -> {e}")

    # 创建并运行爬虫
    crawler = Maga(loop=loop, handler=on_infohash_discovered)
    await crawler.run(port=6881)

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
