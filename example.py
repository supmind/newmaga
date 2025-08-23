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


async def main():
    loop = asyncio.get_running_loop()

    # 定义当爬虫发现新infohash时的回调函数
    async def on_infohash_discovered(infohash, peer_addr):
        infohash_hex = binascii.hexlify(infohash).decode()

        # 如果这个infohash还没有被处理过
        if infohash_hex not in PROCESSED_INFOHASHES:
            PROCESSED_INFOHASHES.add(infohash_hex)
            print(f"[爬虫] 发现: {infohash_hex} | Peer: {peer_addr[0]}:{peer_addr[1]} -> 尝试下载...")

            # 直接调用下载器函数，传入infohash和peer地址
            metadata = await get_metadata(infohash, peer_addr[0], peer_addr[1], loop=loop)

            if metadata:
                # 提取核心信息
                info = metadata.get(b'info', {})
                name = info.get(b'name', b'Unknown').decode(errors='ignore')

                # 将元数据保存到.torrent文件
                file_path = os.path.join("torrents", f"{infohash_hex}.torrent")
                try:
                    with open(file_path, "wb") as f:
                        f.write(bencoder.bencode(metadata))
                    print(f"[成功] 下载并保存了 '{name}' -> {file_path}")
                except Exception as e:
                    print(f"[错误] 保存文件时出错: {e}")
            else:
                print(f"[失败] 未能从 {peer_addr[0]} 下载 {infohash_hex}")

    # 创建并运行爬虫
    crawler = Maga(loop=loop, handler=on_infohash_discovered)
    await crawler.run(port=6881)

    print("爬虫服务已启动，正在监听 announce_peer 消息...")
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
