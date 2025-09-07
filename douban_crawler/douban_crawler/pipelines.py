import csv
import os
import json
from douban_crawler.utils import get_node_id
from typing import Dict, Union
from scrapy import Request
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline

class CustomFilesPipeline(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        def _safe_filename(name: str) -> str:
            """将文件名中的非法字符替换为下划线。"""
            if not name:
                return 'untitled'
            invalid_chars = '\\/:*?"<>|\n\r\t'
            return ''.join((ch if ch not in invalid_chars else '_') for ch in name).strip() or 'untitled'
        type = request.meta['type']
        file_extension = request.url.split('.')[-1]
        title = _safe_filename(item.get('title', ''))
        Id = item.get('id', 'noid')
        item[f'{type}_path'] = f'{type}/{title}_{Id}.{file_extension}'
        return item[f'{type}_path']

    def item_completed(self, results, item, info):
        file_urls = [x['url'] for ok, x in results if ok]
        item['has_cover'] = bool(item['cover'] and item['cover'] in file_urls)
        item['has_trailer'] = bool(item['trailer'] and item['trailer'] in file_urls)
        if not item['has_cover']:
            item['cover_path'] = ""
        if not item['has_trailer']:
            item['trailer_path'] = ""
        if (item['cover'] or item['trailer']) and not file_urls:
            raise DropItem("File Downloaded Failed")
        return item

    def get_media_requests(self, item, info):
        if item['cover']:
            yield Request(item['cover'], meta={
                'type': 'cover',
            }, dont_filter=True)
        if item['trailer']:
            yield Request(item['trailer'], meta={
                'type': 'trailer',
            }, dont_filter=True)


class FileCountPipeline:
    def process_item(self, item, spider):
        if item['has_cover']:
            spider.redis_conn.sadd('douban:cover_ids', item['id'])
        if item['has_trailer']:
            spider.redis_conn.sadd('douban:trailer_ids', item['id'])
        cover_count = spider.redis_conn.scard('douban:cover_ids')
        trailer_count = spider.redis_conn.scard('douban:trailer_ids')
        if cover_count >= spider.target_count and trailer_count >= spider.target_count:
            spider.logger.info(f"已达到目标电影数量 {spider.target_count}, 关闭爬虫")
            spider.crawler.engine.close_spider(spider, 'target_reached')
        return item


class DoubanCsvPipeline:
    CSV_HEADERS = [
        'id',            # id
        'title',         # 片名
        'score',         # 评分（字符串/数字字符串）
        'url',           # 详情页链接
        'vote_count',    # 评分人数
        'actor_count',   # 演员数量
        'genres',        # 类型/题材（列表；写入 CSV 时会转为 JSON 字符串）
        'regions',       # 地区（列表；写入 CSV 时会转为 JSON 字符串）
        'release_date',  # 上映日期
        'has_cover',     # 是否有封面图（布尔；写入 CSV 时会转为 0/1）
        'has_trailer',  # 是否有预告片（布尔；写入 CSV 时会转为 0/1）
        'hot_comments',  # 热门短评列表（写入 CSV 时会转为 JSON 字符串）
        'summary',       # 简介文本
        'cover_path',    # 封面图下载路径
        'trailer_path',  # 预告片下载路径
    ]

    def open_spider(self, spider):
        """每个节点独立保存数据文件"""
        self.node_id = get_node_id()
        self.filename = f"data/douban_movies_{self.node_id}.csv"
        self.file = open(self.filename, 'a', newline='', encoding='utf-8-sig')

        # 需要检查是否是新文件来决定是否写表头
        is_new_file = not os.path.exists(self.filename) or os.path.getsize(self.filename) == 0
        self.writer = csv.DictWriter(self.file, fieldnames=self.CSV_HEADERS)
        if is_new_file:
            self.writer.writeheader()

        # 初始化计数器
        self.count = 0
        self.cover_count = 0
        self.trailer_count = 0

    def close_spider(self, spider):
        self.file.close()
        spider.logger.info(f"保存了 {self.count} 部电影信息到 {self.filename}\n"
                           f"下载封面数：{self.cover_count}，预告片数：{self.trailer_count}")

    def process_item(self, item, spider):
        """写入CSV格式数据"""

        def normalize_row(row: Dict) -> Dict:
            """
            规范化行数据：
            - 布尔转 0/1
            - 列表转 JSON 字符串
            - 缺失字段补空串
            """
            normalized: Dict[str, Union[str, int]] = {}
            for key in self.CSV_HEADERS:
                val = row.get(key)
                if isinstance(val, bool):
                    normalized[key] = 1 if val else 0
                elif isinstance(val, (list, dict)):
                    normalized[key] = json.dumps(val, ensure_ascii=False)
                elif val is None:
                    normalized[key] = ''
                else:
                    normalized[key] = val
            return normalized
        # 格式化数据
        row = normalize_row(item)

        self.writer.writerow(row)
        self.count += 1
        self.cover_count += row['has_cover']
        self.trailer_count += row['has_trailer']
        return item