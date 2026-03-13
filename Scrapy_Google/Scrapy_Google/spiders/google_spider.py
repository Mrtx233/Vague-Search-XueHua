import scrapy
import json
import os
import redis
from urllib.parse import quote
from Scrapy_Google.items import GoogleFileItem
import re

class GoogleSpider(scrapy.Spider):
    name = 'google_spider'
    allowed_domains = ['google.com']
    
    # 初始化配置
    custom_settings = {
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': False}, # 开启有界面模式，方便观察和处理验证码
    }

    def __init__(self, keyword_path=None, *args, **kwargs):
        super(GoogleSpider, self).__init__(*args, **kwargs)
        self.keyword_path = keyword_path or r"E:\Crawler\模糊搜索\模糊搜索\json\output\阿拉伯语\互联网科技_A.json"
        
        # 1. 在 Spider 初始化时建立 Redis 连接，用于关键词去重
        self.rds = redis.Redis(
            host='10.229.32.166',
            port=6379,
            db=6,
            decode_responses=True
        )
        self.redis_prefix = "crawler"

    def is_finished_google(self, keyword):
        """对应原脚本: bool(rds.sismember(f"{REDIS_PREFIX}:keyword_finished:google", keyword))"""
        return self.rds.sismember(f"{self.redis_prefix}:keyword_finished:google", keyword)

    def mark_finished_google(self, keyword):
        """对应原脚本: rds.sadd(f"{REDIS_PREFIX}:keyword_finished:google", keyword)"""
        self.rds.sadd(f"{self.redis_prefix}:keyword_finished:google", keyword)
        self.logger.info(f"关键词已处理完成并标记: {keyword}")

    def start_requests(self):
        """
        1. 加载关键词列表
        2. 检查 Redis，跳过已完成的关键词
        3. 生成搜索请求
        """
        keywords = self.load_keywords(self.keyword_path)
        for kw in keywords:
            # 2. 关键词级别的去重逻辑
            if self.is_finished_google(kw):
                self.logger.info(f"跳过已完成关键词: {kw}")
                continue
            
            # 构造搜索指令
            search_query = f'"{kw}" filetype:xlsx'
            url = f"https://www.google.com/search?q={quote(search_query)}"
            
            yield scrapy.Request(
                url, 
                callback=self.parse, 
                meta={
                    'keyword': kw,
                    'playwright': True 
                }
            )

    def parse(self, response):
        """
        解析搜索结果页，提取文件链接和基本信息
        """
        # 对应原脚本中 parse_search_results 的解析逻辑
        results = response.xpath('//div[@class="N54PNb BToiNc"]')
        
        if not results:
            self.logger.warning(f"关键词 '{response.meta['keyword']}' 未找到结果")
            return

        for res in results:
            item = GoogleFileItem()
            
            # 提取 URL 和 标题
            item['url'] = res.xpath('.//div[@class="yuRUbf"]//a/@href').get()
            title_parts = res.xpath('.//h3//text()').getall()
            item['title'] = "".join(title_parts).strip()
            item['keyword'] = response.meta['keyword']
            
            # 提取文件类型 (从 URL 后缀提取)
            clean_url = item['url'].split('?')[0].split('#')[0]
            ext_match = re.search(r'\.([a-zA-Z0-9]{1,10})$', clean_url)
            item['file_type'] = ext_match.group(1).lower() if ext_match else "xlsx"
            
            # 提取来源网站 (Cite 标签内容)
            item['website'] = res.xpath('.//div[@class="byrV5b"]/cite/text()').get()
            
            yield item

        # 处理翻页逻辑 (寻找 'Next' 按钮)
        next_page = response.xpath('(//td[@class="d6cvqb BBwThe"])[2]/a/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse, meta=response.meta)
        else:
            # 翻页结束，标记当前关键词已完成 (对应原脚本 mark_finished_google)
            self.mark_finished_google(response.meta['keyword'])

    def load_keywords(self, path):
        """
        从本地 JSON 文件加载关键词，并过滤掉空值
        """
        try:
            if not os.path.exists(path):
                self.logger.error(f"关键词文件不存在: {path}")
                return []
            with open(path, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
                # 过滤掉 None 或空字符串
                return [item['外文'] for item in data if item.get('外文')]
        except Exception as e:
            self.logger.error(f"加载关键词异常: {e}")
            return []
