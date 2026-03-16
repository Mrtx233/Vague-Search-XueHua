import scrapy


class GoogleFileItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    file_type = scrapy.Field()
    keyword = scrapy.Field()
    website = scrapy.Field()
    file_hash = scrapy.Field()
    snowflake_id = scrapy.Field()
    language = scrapy.Field()
    domain_class = scrapy.Field()
    crawl_time = scrapy.Field()
    local_path = scrapy.Field()
