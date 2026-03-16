import json
import os
import re
from urllib.parse import quote

import redis
import scrapy

from Scrapy_Google.items import GoogleFileItem


class GoogleSpider(scrapy.Spider):
    name = 'google_spider'
    allowed_domains = ['google.com']

    custom_settings = {
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': False},
    }

    def __init__(self, keyword_path=None, *args, **kwargs):
        super(GoogleSpider, self).__init__(*args, **kwargs)
        self.keyword_path = keyword_path or r"D:\code_Python\Vague-Search-XueHua\json\output\印地语\IT_A.json"
        self.rds = None
        self.redis_prefix = None

    def _init_redis(self):
        if self.rds is None:
            self.rds = redis.Redis(
                host=self.settings.get('REDIS_HOST', '10.229.32.166'),
                port=self.settings.get('REDIS_PORT', 6379),
                db=2,
                decode_responses=True
            )
            self.redis_prefix = self.settings.get('REDIS_PREFIX', 'crawler')

    def is_finished_google(self, keyword):
        return self.rds.sismember(f"{self.redis_prefix}:keyword_finished:google", keyword)

    def mark_finished_google(self, keyword):
        self.rds.sadd(f"{self.redis_prefix}:keyword_finished:google", keyword)
        self.logger.info(f"关键字已处理完成并标记 {keyword}")

    def start_requests(self):
        self._init_redis()
        keywords = self.load_keywords(self.keyword_path)
        for kw in keywords:
            if self.is_finished_google(kw):
                self.logger.info(f"跳过已完成关键词: {kw}")
                continue
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

    async def parse(self, response):
        page = response.meta.get("playwright_page")

        if "检测到您的计算机网络中存在异常流量" in response.text or response.css('form#captcha-form') or "确认您不是机器人" in response.text:
            self.logger.error(f"Captcha triggered for keyword '{response.meta['keyword']}'")
            if page:
                await page.bring_to_front()
                self.logger.info("等待人工处理验证码 60 秒")
                await page.wait_for_timeout(60000)
            yield scrapy.Request(
                response.url,
                callback=self.parse,
                meta=response.meta,
                dont_filter=True,
                priority=10
            )
            return

        results = response.xpath('//div[@class="N54PNb BToiNc"]')
        if not results:
            self.logger.warning(f"关键词'{response.meta['keyword']}'未找到结果")
            return

        for res in results:
            item = GoogleFileItem()
            item['url'] = res.xpath('.//div[@class="yuRUbf"]//a/@href').get()
            title_parts = res.xpath('.//h3//text()').getall()
            item['title'] = "".join(title_parts).strip()
            item['keyword'] = response.meta['keyword']

            clean_url = item['url'].split('?')[0].split('#')[0]
            ext_match = re.search(r'\.([a-zA-Z0-9]{1,10})$', clean_url)
            item['file_type'] = ext_match.group(1).lower() if ext_match else "xlsx"

            item['website'] = res.xpath('.//div[@class="byrV5b"]/cite/text()').get()

            yield item

        next_page = response.xpath('(//td[@class="d6cvqb BBwThe"])[2]/a/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse, meta=response.meta)
        else:
            self.mark_finished_google(response.meta['keyword'])

    def load_keywords(self, path):
        try:
            if not os.path.exists(path):
                self.logger.error(f"关键字文件不存在: {path}")
                return []
            with open(path, 'r', encoding='utf-8-sig') as f:
                data = json.load(f)
                return [item['外文'] for item in data if item.get('外文')]
        except Exception as e:
            self.logger.error(f"加载关键字异常 {e}")
            return []
