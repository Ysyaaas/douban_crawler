import aiohttp
import logging

import aiohttp
import logging
from scrapy import Request
from typing import Dict, Optional


class ProxyMiddleware:
    """异步代理中间件，直接从代理池获取代理"""

    def __init__(self, settings):
        self.logger = logging.getLogger(__name__)
        self.proxypool_url = settings.get('PROXYPOOL_URL')
        self.timeout = aiohttp.ClientTimeout(total=10)
        self.session = None  # 将在爬虫打开时创建
        self.stats = {
            'total_requests': 0,
            'proxy_used': 0,
            'proxy_failures': 0
        }

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    async def spider_opened(self, spider):
        """爬虫打开时创建共享的ClientSession"""
        self.session = aiohttp.ClientSession(timeout=self.timeout)
        spider.logger.info("Proxy middleware initialized with shared session")

    async def spider_closed(self, spider, reason):
        """爬虫关闭时关闭会话"""
        if self.session:
            await self.session.close()
            spider.logger.info("Proxy session closed")

        # 输出统计信息
        spider.logger.info(f"Proxy usage stats: "
                           f"Total requests: {self.stats['total_requests']}, "
                           f"Proxy used: {self.stats['proxy_used']}, "
                           f"Proxy failures: {self.stats['proxy_failures']}")

    async def process_request(self, request: Request, spider):
        """处理请求，设置代理"""
        self.stats['total_requests'] += 1

        # 跳过已经设置代理的请求
        if 'proxy' in request.meta:
            return

        # 获取代理
        try:
            async with self.session.get(self.proxypool_url) as response:
                if response.status != 200:
                    spider.logger.warning(f"Proxy pool returned {response.status}")
                    return

                # 解析代理信息
                proxy_info = await response.json()
                if not proxy_info or 'proxy' not in proxy_info:
                    spider.logger.warning("Invalid proxy info received")
                    return

                # 设置代理
                proxy_address = proxy_info['proxy']
                request.meta['proxy'] = f'http://{proxy_address}'
                request.meta['proxy_info'] = proxy_info

                self.stats['proxy_used'] += 1
                spider.logger.debug(f"Using proxy: {proxy_address} for {request.url}")
        except Exception as e:
            self.stats['proxy_failures'] += 1
            spider.logger.error(f"Failed to get proxy: {str(e)}")
