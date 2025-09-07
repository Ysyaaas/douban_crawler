BOT_NAME = 'douban_crawler'

# 分布式核心配置
SCHEDULER = "scrapy_redis.scheduler.Scheduler"
DUPEFILTER_CLASS = "scrapy_redis.dupefilter.RFPDupeFilter"
# SCHEDULER_QUEUE_CLASS = 'scrapy_redis.queue.PriorityQueue'

SCHEDULER_PERSIST = True  # 暂停后保持队列

# Redis连接配置
REDIS_HOST = '10.109.253.xxx'  # Redis服务器IP (四卡)
REDIS_PORT = 6379

# 代理配置
PROXYPOOL_URL = 'http://10.109.253.xxx:5010/get/'

# 启用组件 数字表示组件优先级，越小优先级越高
ITEM_PIPELINES = {
    "douban_crawler.pipelines.CustomFilesPipeline": 300,
    "douban_crawler.pipelines.FileCountPipeline": 301,
    'douban_crawler.pipelines.DoubanCsvPipeline': 302,
}
FILES_STORE = './data'
IMAGES_STORE = './images'
LOG_LEVEL = 'DEBUG'
# 启用监控扩展
EXTENSIONS = {
    'douban_crawler.extensions.StatusExtension': 500,
}
# 爬虫设置
ROBOTSTXT_OBEY = False  # 忽略robots.txt
CONCURRENT_REQUESTS = 2
DOWNLOAD_DELAY = 2.5  # 避免被封
# RANDOMIZE_DOWNLOAD_DELAY = True  # 在延迟范围内随机化
# AUTOTHROTTLE_ENABLED = True
# AUTOTHROTTLE_START_DELAY = 5.0
# AUTOTHROTTLE_MAX_DELAY = 60.0
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

# 目标电影数量
TARGET_MOVIE_COUNT = 2000
SPIDER_MODULES = ['douban_crawler.spiders']

SCHEDULER_BATCH_SIZE = 1  # 每次只取1个请求

DOWNLOADER_MIDDLEWARES = {
   # "douban_crawler.middlewares.ProxyMiddleware": 543,
}
