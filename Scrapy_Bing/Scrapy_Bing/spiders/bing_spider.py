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
    
    # 初始化配置
    custom_settings = {
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': False}, # 开启有界面模式
    }

    def __init__(self, keyword_path=None, *args, **kwargs):
        super(BingSpider, self).__init__(*args, **kwargs)
        # 关键词路径
        self.keyword_path = keyword_path or r"E:\Crawler\模糊搜索\模糊搜索\json\output\泰语\IT_A.json"
        
        # Redis 连接用于关键词去重
        self.rds = redis.Redis(
            host='10.229.32.166',
            port=6379,
            db=6,
            decode_responses=True
        )
        self.redis_prefix = "crawler"

    def is_finished_bing(self, keyword):
        """对应原脚本: bool(rds.sismember(f"{REDIS_PREFIX}:keyword_finished:bing", keyword))"""
        return self.rds.sismember(f"{self.redis_prefix}:keyword_finished:bing", keyword)

    def mark_finished_bing(self, keyword):
        """对应原脚本: rds.sadd(f"{REDIS_PREFIX}:keyword_finished:bing", keyword)"""
        self.rds.sadd(f"{self.redis_prefix}:keyword_finished:bing", keyword)
        self.logger.info(f"关键词已处理完成并标记: {keyword}")

    def start_requests(self):
        """
        1. 加载关键词列表
        2. 检查 Redis，跳过已完成的关键词
        3. 生成搜索请求
        """
        keywords = self.load_keywords(self.keyword_path)
        for kw in keywords:
            # 关键词去重
            if self.is_finished_bing(kw):
                self.logger.info(f"跳过已完成关键词: {kw}")
                continue
            
            # 构造搜索指令: "关键词" filetype:xlsx
            search_query = f'"{kw}" filetype:xlsx'
            url = f"https://www.bing.com/search?q={quote(search_query)}"
            
            yield scrapy.Request(
                url, 
                callback=self.parse, 
                meta={
                    'keyword': kw,
                    'playwright': True 
                }
            )

    async def parse(self, response):
        """
        解析搜索结果页，增加验证码检测逻辑 (Bing 也有可能有拦截)
        """
        page = response.meta.get("playwright_page")
        
        # 1. 检测是否触发了验证码或异常访问
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

        # 2. 正常解析逻辑
        # Bing 的搜索结果通常在 <li class="b_algo"> 中
        results = response.xpath('//li[@class="b_algo"]')
        
        if not results:
            self.logger.warning(f"关键词 '{response.meta['keyword']}' 未找到结果")
            # 如果确实没有结果，也要标记完成
            self.mark_finished_bing(response.meta['keyword'])
            return

        for res in results:
            item = BingFileItem()
            
            # 提取 URL 和 标题
            item['url'] = res.xpath('.//h2/a/@href').get()
            title_parts = res.xpath('.//h2/a//text()').getall()
            item['title'] = "".join(title_parts).strip()
            item['keyword'] = response.meta['keyword']
            
            if not item['url']:
                continue

            # 处理文件类型
            clean_url = item['url'].split('?')[0].split('#')[0]
            ext_match = re.search(r'\.([a-zA-Z0-9]{1,10})$', clean_url)
            item['file_type'] = ext_match.group(1).lower() if ext_match else "xlsx"
            
            # 提取来源网站域名
            try:
                item['website'] = urlparse(item['url']).netloc
            except:
                item['website'] = "unknown"
            
            yield item

        # 处理翻页逻辑 (寻找 'Next' 按钮)
        next_page = response.xpath('//a[@title="下一页"]/@href').get() or response.xpath('//a[@title="Next page"]/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse, meta=response.meta)
        else:
            # 翻页结束，标记当前关键词已完成
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
