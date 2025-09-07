import json

import redis
import time
from tabulate import tabulate
from datetime import datetime


def monitor_cluster(redis_host='localhost', redis_port=6379):
    r = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    while True:
        # 清屏
        print("\033c", end="")

        # 获取队列状态
        queue_size = r.zcard('douban:requests')
        movie_count = r.scard('douban:movie_ids')
        dupefilter_size = r.scard('douban:dupefilter')
        cover_count = r.scard('douban:cover_ids')
        trailer_count = r.scard('douban:trailer_ids')

        # 获取节点状态
        nodes = r.hgetall('crawler:nodes')
        stats = []
        active_nodes = 0

        for node_id, status_str in nodes.items():
            try:
                status_data = json.loads(status_str)
                last_update = status_data.get('last_update', 0)

                # 检查是否活跃（最近30秒内有更新）
                is_active = time.time() - last_update < 30
                if is_active:
                    active_nodes += 1

                # 格式化最后更新时间
                last_time = datetime.fromtimestamp(last_update).strftime('%H:%M:%S') if last_update else 'N/A'

                stats.append([
                    node_id,
                    status_data.get('requests', 'N/A'),
                    status_data.get('items', 'N/A'),
                    status_data.get('cover', 'N/A'),
                    status_data.get('trailer', 'N/A'),
                    last_time,
                    '✓' if is_active else '✗'
                ])
            except Exception as e:
                stats.append([node_id, 'ERROR', 'ERROR', 'ERROR', 'ERROR', 'ERROR', 'ERROR'])

        # 打印监控信息
        print(f"==== Douban Movie Crawler Monitor ====")
        print(f"Redis Server: {redis_host}:{redis_port}")
        print(f"URL Queue: {queue_size} | Unique Movies: {movie_count} "
              f"| Covers: {cover_count} | Trailers: {trailer_count}")
        print(f"Active Nodes: {active_nodes}/{len(nodes)}\n")

        # 打印节点状态表格
        print(tabulate(stats,
                       headers=['Node ID', 'Requests', 'Items', 'Covers', 'Trailers', 'Last Update', 'Active'],
                       tablefmt='grid'))

        # 打印当前时间
        print(f"\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("Press Ctrl+C to exit...")

        time.sleep(2)  # 每2秒刷新一次


if __name__ == "__main__":
    redis_host = "10.109.253.108"
    try:
        monitor_cluster(redis_host=redis_host)
    except KeyboardInterrupt:
        print("\nMonitoring stopped")
    except redis.ConnectionError:
        print("Error: Could not connect to Redis server")
