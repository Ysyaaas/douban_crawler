import scrapy
import scrapy_redis.queue
from scrapy_redis.spiders import RedisSpider
from urllib.parse import urlparse, parse_qs
from douban_crawler.items import DoubanMovieItem
import json
import redis


class DoubanSpider(RedisSpider):
    name = "douban"
    # redis_key = "douban:requests"  # 已重写start_requests方法，此属性失效

    # 电影类型映射
    MOVIE_TYPES = {
        1: "纪录片", 2: "传记", 3: "犯罪", 4: "历史", 5: "动作",
        6: "情色", 7: "歌舞", 8: "儿童", 9: "", 10: "悬疑",
        11: "剧情", 12: "灾难", 13: "爱情", 14: "音乐", 15: "冒险",
        16: "奇幻", 17: "科幻", 18: "运动", 19: "惊悚", 20: "恐怖",
        21: "", 22: "战争", 23: "短片", 24: "喜剧", 25: "动画",
        26: "", 27: "西部", 28: "家庭", 29: "武侠", 30: "古装",
        31: "黑色电影"
    }

    # 区间列表
    INTERVALS = [
        "100:90", "90:80", "80:70", "70:60", "60:50",
        "50:40", "40:30", "30:20", "20:10", "10:0"
    ]

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """覆盖from_crawler方法以获取设置"""
        spider = super().from_crawler(crawler, *args, **kwargs)
        # 从crawler获取设置
        spider.redis_host = crawler.settings.get('REDIS_HOST')
        spider.redis_port = crawler.settings.get('REDIS_PORT')
        spider.target_count = crawler.settings.getint('TARGET_MOVIE_COUNT', 10000)

        # 初始化Redis连接
        spider.redis_conn = redis.Redis(
            host=spider.redis_host,
            port=spider.redis_port,
            decode_responses=True  # 自动解码为字符串
        )

        spider.logger.info(f"目标电影数量: {spider.target_count}")
        return spider


    def start_requests(self):
        """生成初始请求（使用优先级队列）"""
        # 获取最高优先级的URL
        for movie_type in range(1, 32):  # 类型1-31
            url = f"https://movie.douban.com/j/chart/top_list?type={movie_type}&interval_id=100:90&action=&start=0&limit=100"
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        """解析豆瓣电影API响应"""
        # 解析当前URL参数
        parsed = urlparse(response.url)
        params = parse_qs(parsed.query)
        movie_type = int(params['type'][0])
        interval_id = params['interval_id'][0]
        start = int(params['start'][0])
        # 解析JSON响应
        try:
            movies = json.loads(response.text)
        except json.JSONDecodeError:
            self.logger.error(f"JSON解析失败: {response.url}")
            return

        # 检查爬取目标是否达到，若达到则关闭爬虫
        self.check_target_reached()

        # 处理电影数据
        for movie_data in movies:
            movie_id = movie_data['id']
            # 去重检查
            if self.redis_conn.sadd('douban:movie_ids', movie_id) == 0:
                continue  # 已存在，跳过

            meta = {
                'id': movie_id,
                'title': movie_data.get('title', ''),
                'score': movie_data.get('score', ''),
                'vote_count': movie_data.get('vote_count', ''),
                'actor_count': movie_data.get('actor_count', ''),
                'url': movie_data.get('url', ''),
                'genres': movie_data.get('types', ''),
                'regions': movie_data.get('regions', ''),
                'release_date': movie_data.get('release_date', ''),
                'has_trailer': False,
                'has_cover': False,
                'cover': None,
                'trailer': None,
                'hot_comments': [],
                'summary': None,
                'cover_path': None,
                'trailer_path': None,
            }

            # 生成新请求，详情页
            if movie_data.get('url', ''):
                yield scrapy.Request(movie_data['url'], callback=self.parse_detail, meta=meta, priority=int(1e9))

        # 生成新请求，下一top list页
        yield from self.generate_next_requests(movie_type, interval_id, start, len(movies))

    def parse_detail(self, response):
        meta = response.meta
        # 封面
        meta['cover'] = response.css('meta[property="og:image"]::attr(content)').get()
        # 热门评论
        meta['hot_comments'] = response.css('div#hot-comments span.short::text').getall()
        # 简介
        intro_text = ' '.join(
            response.css('#link-report-intra .all.hidden *::text').getall() or
            response.css('#link-report-intra [property="v:summary"] *::text').getall() or
            response.css('#link-report-intra .short [property="v:summary"] *::text').getall() or
            response.css('#link-report-intra .indent *::text').getall()
        )

        # 深度清理文本
        if intro_text:
            # 合并多余空白（但保留换行）
            intro_text = '\n'.join(
                line.strip()
                for line in intro_text.split('\n')
                if line.strip()
            )
        meta['summary'] = intro_text
        trailer_href = response.css('a.related-pic-video::attr(href)').get()
        if trailer_href:
            yield scrapy.Request(trailer_href, callback=self.parse_video, meta=meta, priority=int(1e9))
        else:
            yield self.create_item_from_dict(DoubanMovieItem, meta)


    def parse_video(self, response):
        meta = response.meta
        meta['trailer'] = response.css('video source::attr(src)').get()
        yield self.create_item_from_dict(DoubanMovieItem, meta)

    def create_item_from_dict(self, item_class, data_dict):
        """
        使用字典数据创建一个Scrapy Item实例

        参数:
            item_class: Scrapy Item类 (继承自scrapy.Item)
            data_dict: 包含字段数据的字典

        返回:
            实例化的Item对象
        """
        if not issubclass(item_class, scrapy.Item):
            raise ValueError("item_class 必须是scrapy.Item的子类")

        # 创建Item实例
        item = item_class()

        # 验证并设置字段
        for field_name, value in data_dict.items():
            if field_name in item_class.fields:
                item[field_name] = value
        return item

    def check_target_reached(self):
        cover_count = self.redis_conn.scard('douban:cover_ids')
        trailer_count = self.redis_conn.scard('douban:trailer_ids')
        if cover_count >= self.target_count and trailer_count >= self.target_count:
            self.logger.info(f"已达到目标电影数量 {self.target_count}, 关闭爬虫")
            self.crawler.engine.close_spider(self, 'target_reached')

    def generate_next_requests(self, movie_type, interval_id, start, movie_count):
        """生成下一个请求"""
        # 当前区间索引
        interval_idx = self.INTERVALS.index(interval_id)

        # 如果当前页返回的电影数量不足100，说明当前区间结束
        if movie_count < 100:
            # 尝试下一个区间
            if interval_idx + 1 < len(self.INTERVALS):
                next_interval = self.INTERVALS[interval_idx + 1]
                next_start = 0
                next_url = self.build_url(movie_type, next_interval, next_start)
                priority = self.calculate_priority(next_interval, next_start)
                yield scrapy.Request(next_url, callback=self.parse, priority=priority)
        else:
            # 当前区间还有下一页
            next_start = start + 100
            next_url = self.build_url(movie_type, interval_id, next_start)
            priority = self.calculate_priority(interval_id, next_start)
            yield scrapy.Request(next_url, callback=self.parse, priority=priority)

    def build_url(self, movie_type, interval_id, start):
        """构建API URL"""
        return f"https://movie.douban.com/j/chart/top_list?type={movie_type}&interval_id={interval_id}&action=&start={start}&limit=100"

    def calculate_priority(self, interval_id, start):
        """计算请求优先级"""
        # 区间优先级：100:90 > 90:80 > ... > 10:0
        interval_value = int(interval_id.split(':')[0])
        page = start // 100
        score = (100 - interval_value) * 1000 + page
        return -score

