import time
import json
import redis
from scrapy import signals
from douban_crawler.utils import get_node_id


class StatusExtension:
    """节点状态监控扩展"""

    def __init__(self, stats, redis_host, redis_port, item_count):
        self.stats = stats
        self.redis_conn = redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            decode_responses=True
        )
        self.node_id = get_node_id()
        self.last_report_time = 0
        self.item_count = item_count
        self.items_scraped = 0
        self.cover_count = 0
        self.trailer_count = 0

    @classmethod
    def from_crawler(cls, crawler):
        # 从设置获取Redis配置
        ext = cls(
            stats=crawler.stats,
            redis_host=crawler.settings.get('REDIS_HOST'),
            redis_port=crawler.settings.get('REDIS_PORT'),
            item_count=5
        )
        # 注册信号
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_idle, signal=signals.spider_idle)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.item_scraped, signal=signals.item_scraped)
        return ext

    def spider_opened(self, spider):
        """爬虫启动时注册节点"""
        self.report_status('starting')

    def spider_idle(self, spider):
        """爬虫空闲时定期上报状态"""
        current_time = time.time()
        if current_time - self.last_report_time >= 10:  # 每10秒上报一次
            self.report_status('running')

    def spider_closed(self, spider, reason):
        """爬虫关闭时注销节点"""
        self.redis_conn.hdel('crawler:nodes', self.node_id)

    def item_scraped(self, item, spider):
        self.items_scraped += 1
        self.cover_count += item['has_cover']
        self.trailer_count += item['has_trailer']
        current_time = time.time()
        if current_time - self.last_report_time >= 10:  # 每10秒上报一次
            self.report_status('running')
        # if self.items_scraped % self.item_count == 0:
        #     self.report_status('running')

    def report_status(self, status):
        """上报状态到Redis"""
        stats_data = {
            'status': status,
            'requests': self.stats.get_value('downloader/request_count', 0),
            'items': self.stats.get_value('item_scraped_count', 0),
            'last_update': int(time.time()),
            'cover': self.cover_count,
            'trailer': self.trailer_count,
        }
        # 保存状态到Redis的Hash表
        self.redis_conn.hset('crawler:nodes', self.node_id, json.dumps(stats_data))
        self.last_report_time = time.time()