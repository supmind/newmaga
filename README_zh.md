# Maga: 一个高性能 DHT 爬虫框架

Maga 是一个功能强大且易于使用的 DHT 爬虫构建框架，使用 Python 的 `asyncio` 编写，并通过 `uvloop` 加速。它允许您监听 BitTorrent DHT 网络，发现 infohash，并获取种子的元数据。

## 功能特性

*   **高性能**: 基于 `asyncio` 和 `uvloop` 构建，具有出色的性能和可扩展性。
*   **简洁的 API**: 只需几行代码即可运行一个爬虫。
*   **可扩展**: 提供简单的处理器 (`handle_get_peers`, `handle_announce_peer`)，您可以覆盖它们来处理传入的数据。
*   **元数据下载器**: 包含一个实用工具，可以轻松地从发现的节点下载种子元数据。
*   **实用工具**: 自带用于诊断和测试的工具。

## 安装

1.  克隆仓库:

    ```bash
    git clone https://github.com/whtsky/maga.git
    cd maga
    ```

2.  安装所需的依赖:

    ```bash
    pip install -r requirements.txt
    ```

3.  以可编辑模式安装项目，以确保模块在您的路径中可用:

    ```bash
    pip install -e .
    ```

## 快速开始

我们提供了一个新的示例 `maga_save.py`，这是一个功能完善的元数据爬虫。它采用生产者-消费者模型，其中爬虫发现 infohash，而一个工作池则并发下载元数据，并将其保存到 `downloads` 文件夹中。

要运行它:

```bash
python maga_save.py --port 6881 --workers 200
```

您可以自定义 DHT 监听端口和下载工作者的数量。

以下是 `maga_save.py` 中代码的简化版本:

```python
import asyncio
import logging
import os
from maga.crawler import Maga
from maga.downloader import get_metadata
from maga.utils import proper_infohash
from fastbencode import bencode

# 配置日志
logging.basicConfig(level=logging.INFO)

# 定义下载目录
DOWNLOAD_DIR = "downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

class MyCrawler(Maga):
    async def handler(self, infohash, addr, peer_addr=None):
        # 这个处理器用于处理 announce_peer 消息
        if not peer_addr:
            return

        infohash_hex = proper_infohash(infohash)
        print(f"从 {peer_addr} 发现了 {infohash_hex}")

        # 现在你可以下载元数据
        info = await get_metadata(infohash, peer_addr[0], peer_addr[1])
        if info:
            print(f"成功下载 {infohash_hex} 的元数据")
            # 将元数据保存为 .torrent 文件
            torrent_data = bencode(info)
            file_path = os.path.join(DOWNLOAD_DIR, f"{infohash_hex}.torrent")
            with open(file_path, "wb") as f:
                f.write(torrent_data)

async def main():
    crawler = MyCrawler()
    await crawler.run()
    # 爬虫将永远运行，直到被停止
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
```

## API 概述

要构建自己的爬虫，您只需子类化 `maga.Maga` 并覆盖其一个或多个处理器：

*   `async def handler(self, infohash, addr, peer_addr=None)`: 一个高级处理器，用于处理 `announce_peer` 消息。这是最简单的入门方式。
*   `async def handle_get_peers(self, infohash, addr)`: 当从另一个节点收到 `get_peers` 查询时调用此处理器。`addr` 是查询节点的地址。
*   `async def handle_announce_peer(self, infohash, addr, peer_addr)`: 当收到 `announce_peer` 查询时调用此处理器。`addr` 是宣告的 DHT 节点的地址，`peer_addr` 是作为种子群一部分的对等节点的地址。
