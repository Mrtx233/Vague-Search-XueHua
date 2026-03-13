import scrapy
import json
import os
import redis
from urllib.parse import quote, urlparse
from Scrapy_Bing.items import BingFileItem
import re

class BingSpider(scrapy.Spider):
    name = 'bing_spider'
    allowed_domains = ['bing.com']

    def __init__(self, keyword_path=None, *args, **kwargs):
        super(BingSpider, self).__init__(*args, **kwargs)
        self.keyword_path = keyword_path or r"E:\Crawler\模糊搜索\模糊搜索\json\output\泰语\IT_A.json"

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider.rds = redis.Redis(
            host=crawler.settings.get('REDIS_HOST', '10.229.32.166'),
            port=crawler.settings.get('REDIS_PORT', 6379),
            db=crawler.settings.get('REDIS_DB', 6),
            decode_responses=True
        )
        spider.redis_prefix = crawler.settings.get('REDIS_PREFIX', 'crawler')
        return spider

    def is_finished_bing(self, keyword):
        """对应原脚本: bool(rds.sismember(f"{REDIS_PREFIX}:keyword_finished:bing", keyword))"""
        return self.rds.sismember(f"{self.redis_prefix}:keyword_finished:bing", keyword)

    def mark_finished_bing(self, keyword):
        """对应原脚本: rds.sadd(f"{REDIS_PREFIX}:keyword_finished:bing", keyword)"""
        self.rds.sadd(f"{self.redis_prefix}:keyword_finished:bing", keyword)
        self.logger.info(f"关键词已处理完成并标记: {keyword}")

    def start_requests(self):
        keywords = self.load_keywords(self.keyword_path)
        for kw in keywords:
            if self.is_finished_bing(kw):
                self.logger.info(f"跳过已完成关键词: {kw}")
                continue

            search_query = f'"{kw}" filetype:xlsx'
            url = f"https://www.bing.com/search?q={quote(search_query)}"

            yield scrapy.Request(
                url,
                callback=self.parse,
                meta={
                    'keyword': kw,
                    'playwright': True,
                    'playwright_context': 'default',
                }
            )

    async def parse(self, response):
        page = response.meta.get("playwright_page")

        if "我们的系统检测到您的计算机网络中存在异常流量" in response.text or "确认您不是机器人" in response.text:
            self.logger.error(f"⚠️ 拦截：关键词 '{response.meta['keyword']}' 触发验证码！")
            if page:
                await page.bring_to_front()
                self.logger.info("浏览器已暂停，请在 60 秒内完成验证...")
                await page.wait_for_timeout(60000)

            yield scrapy.Request(
                response.url,
                callback=self.parse,
                meta=response.meta,
                dont_filter=True,
                priority=10
            )
            return

        results = response.xpath('//li[@class="b_algo"]')

        if not results:
            self.logger.warning(f"关键词 '{response.meta['keyword']}' 未找到结果")
            self.mark_finished_bing(response.meta['keyword'])
            return

        for res in results:
            item = BingFileItem()

            item['url'] = res.xpath('.//h2/a/@href').get()
            title_parts = res.xpath('.//h2/a//text()').getall()
            item['title'] = "".join(title_parts).strip()
            item['keyword'] = response.meta['keyword']

            if not item['url']:
                continue

            clean_url = item['url'].split('?')[0].split('#')[0]
            ext_match = re.search(r'\.([a-zA-Z0-9]{1,10})$', clean_url)
            item['file_type'] = ext_match.group(1).lower() if ext_match else "xlsx"

            try:
                item['website'] = urlparse(item['url']).netloc
            except Exception:
                item['website'] = "unknown"

            yield item

        next_page = response.xpath('//a[@title="下一页"]/@href').get() or response.xpath('//a[@title="Next page"]/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse, meta=response.meta)
        else:
            self.mark_finished_bing(response.meta['keyword'])

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
                return [item['外文'] for item in data if item.get('外文')]
        except Exception as e:
            self.logger.error(f"加载关键词异常: {e}")
            return []
