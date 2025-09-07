from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import logging
import os
import redis

# 启用日志记录
# logging.basicConfig(
#     format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
#     level=logging.INFO
# )


def initialize_redis(settings):
    """初始化Redis状态"""
    r = redis.Redis(
        host=settings.get('REDIS_HOST'),
        port=settings.get('REDIS_PORT')
    )

    # 清空旧状态
    r.delete('douban:requests')
    r.delete('douban:dupefilter')
    r.delete('douban:movie_ids')
    r.delete('douban:cover_ids')
    r.delete('douban:trailer_ids')

    # 添加初始URL
    # for movie_type in range(1, 32):  # 类型1-31
    # for movie_type in range(1, 2):  # 类型1-31
    #     url = f"https://movie.douban.com/j/chart/top_list?type={movie_type}&interval_id=100:90&action=&start=0&limit=100"
    #     # r.lpush('douban:requests', url)
    #     r.zadd('douban:requests', {url: 0})
    #
    # logging.info(f"添加了 {31} 个初始URL")


def run_spider():
    settings = get_project_settings()
    # print(settings.get('SCHEDULER_QUEUE_CLASS'))

    # 初始化Redis
    # initialize_redis(settings)

    process = CrawlerProcess(settings)
    process.crawl('douban')
    process.start()


if __name__ == "__main__":
    run_spider()