import scrapy


class DoubanMovieItem(scrapy.Item):
    title = scrapy.Field()  # 电影名称
    id = scrapy.Field()  # 电影id
    score = scrapy.Field()  # 电影评分
    vote_count = scrapy.Field()  # 评分人数
    actor_count = scrapy.Field()  # 演员数量
    url = scrapy.Field()  # 详情页url
    release_date = scrapy.Field()  # 上映日期
    regions = scrapy.Field()  # 地区
    genres = scrapy.Field()  # 类型/题材
    has_trailer = scrapy.Field()  # 是否有预告片
    has_cover = scrapy.Field()  # 是否有封面图
    hot_comments = scrapy.Field()  # 热门短评列表
    summary = scrapy.Field()  # 简介
    cover = scrapy.Field()  # 封面图url
    trailer = scrapy.Field()  # 预告片url
    cover_path = scrapy.Field()  # 封面图本地保存路径
    trailer_path = scrapy.Field()  # 预告片本地保存路径
