import scrapy

class BingFileItem(scrapy.Item):
    # 原始抓取字段
    url = scrapy.Field()          # 文件的源URL
    title = scrapy.Field()        # 搜索结果显示的标题
    file_type = scrapy.Field()    # 文件扩展名 (如 xlsx, pdf)
    keyword = scrapy.Field()      # 触发此次搜索的原始关键词
    website = scrapy.Field()      # 来源网站域名
    
    # 处理后生成的字段 (Pipeline 填充)
    file_hash = scrapy.Field()    # 文件的 MD5 哈希值
    snowflake_id = scrapy.Field() # 生成的 11 位雪花 ID
    language = scrapy.Field()     # 语种检测结果
    domain_class = scrapy.Field() # 域名分类 (如 gov, edu)
    crawl_time = scrapy.Field()   # 抓取时间戳 (ms)
    local_path = scrapy.Field()   # 文件保存的本地路径
