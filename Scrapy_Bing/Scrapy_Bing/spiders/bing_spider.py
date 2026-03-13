import scrapy
import json
import os
import redis
import asyncio
import random
from urllib.parse import quote, urlparse
from Scrapy_Bing.items import BingFileItem
import re

class BingSpider(scrapy.Spider):
    name = 'bing_spider'
    allowed_domains = ['bing.com']

    def __init__(self, keyword_path=None, *args, **kwargs):
        super(BingSpider, self).__init__(*args, **kwargs)
        # 关键词路径
        self.keyword_path = keyword_path or r"E:\Crawler\模糊搜索\模糊搜索\json\output\泰语\IT_A.json"
        
        # Redis 连接用于关键词去重
        self.rds = redis.Redis(
            host='10.229.32.166',
            port=6379,
            db=2,
            decode_responses=True
        )
        self.redis_prefix = "crawler"
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def is_finished_bing(self, keyword):
        """对应原脚本: bool(rds.sismember(f"{REDIS_PREFIX}:keyword_finished:bing", keyword))"""
        return self.rds.sismember(f"{self.redis_prefix}:keyword_finished:bing", keyword)

    def mark_finished_bing(self, keyword):
        """对应原脚本: rds.sadd(f"{REDIS_PREFIX}:keyword_finished:bing", keyword)"""
        self.rds.sadd(f"{self.redis_prefix}:keyword_finished:bing", keyword)
        self.logger.info(f"关键词已处理完成并标记: {keyword}")

    async def start(self):
        keywords = self.load_keywords(self.keyword_path)

        from playwright.async_api import async_playwright

        self._pw = await async_playwright().start()
        self._browser = await self._pw.chromium.launch(headless=False)
        self._context = await self._browser.new_context()
        self._page = await self._context.new_page()

        try:
            from playwright_stealth import stealth_async
            await stealth_async(self._page)
        except Exception:
            pass

        for kw in keywords:
            if self.is_finished_bing(kw):
                self.logger.info(f"跳过已完成关键词: {kw}")
                continue

            async for item in self._crawl_keyword(kw):
                yield item

    async def _crawl_keyword(self, keyword):
        search_query = f'"{keyword}" filetype:xlsx'
        url = f"https://www.bing.com/search?q={quote(search_query)}"

        next_url = url
        while next_url:
            html_response = await self._goto_and_get_response(next_url, keyword)
            if html_response is None:
                return

            results = html_response.xpath('//li[@class="b_algo"]')
            if not results:
                self.logger.warning(f"关键词 '{keyword}' 未找到结果")
                self.mark_finished_bing(keyword)
                return

            for res in results:
                item = BingFileItem()
                item['url'] = res.xpath('.//h2/a/@href').get()
                title_parts = res.xpath('.//h2/a//text()').getall()
                item['title'] = "".join(title_parts).strip()
                item['keyword'] = keyword

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

            next_page = html_response.xpath('//a[@title="下一页"]/@href').get() or html_response.xpath('//a[@title="Next page"]/@href').get()
            if next_page:
                next_url = html_response.urljoin(next_page)
            else:
                next_url = None
                self.mark_finished_bing(keyword)

            await self._human_delay()

    async def _goto_and_get_response(self, url, keyword):
        await self._human_delay()
        await self._page.goto(url, wait_until="domcontentloaded", timeout=120000)
        await self._page.wait_for_timeout(800)
        html = await self._page.content()

        if self._is_captcha_page(html):
            self.logger.error(f"⚠️ 拦截：关键词 '{keyword}' 触发验证码，浏览器将保持打开供人工处理")
            await self._page.bring_to_front()

            start_ts = asyncio.get_event_loop().time()
            while True:
                await asyncio.sleep(1.0)
                html = await self._page.content()
                if not self._is_captcha_page(html):
                    break
                if asyncio.get_event_loop().time() - start_ts > 300:
                    self.logger.error(f"关键词 '{keyword}' 验证码等待超时，跳过该关键词")
                    return None

        return scrapy.http.HtmlResponse(url=self._page.url, body=html.encode("utf-8"), encoding="utf-8")

    def _is_captcha_page(self, html: str) -> bool:
        if not html:
            return False
        return (
            "我们的系统检测到您的计算机网络中存在异常流量" in html
            or "确认您不是机器人" in html
            or "Captcha" in html
            or "验证" in html and "机器人" in html
        )

    async def _human_delay(self):
        base = float(self.settings.get("DOWNLOAD_DELAY", 10))
        jitter = random.uniform(0.5, 1.5)
        await asyncio.sleep(base * jitter)

    def closed(self, reason):
        async def _close():
            try:
                if self._context:
                    await self._context.close()
            finally:
                try:
                    if self._browser:
                        await self._browser.close()
                finally:
                    if self._pw:
                        await self._pw.stop()

        try:
            loop = asyncio.get_event_loop()
            loop.create_task(_close())
        except Exception:
            pass

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
