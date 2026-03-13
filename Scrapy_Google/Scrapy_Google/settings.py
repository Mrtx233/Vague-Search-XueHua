# Scrapy settings for Scrapy_Google project
import os

BOT_NAME = "Scrapy_Google"

SPIDER_MODULES = ["Scrapy_Google.spiders"]
NEWSPIDER_MODULE = "Scrapy_Google.spiders"

# 1. 设置 User-Agent (对应原脚本 USER_AGENT)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'

# 2. 遵守 robots.txt (Google 爬取通常需要设为 False)
ROBOTSTXT_OBEY = False

# 3. 配置并发与延迟 (对应原脚本 MAX_WORKERS 和 time.sleep)
CONCURRENT_REQUESTS = 8
DOWNLOAD_DELAY = 2  # 基础延迟 2 秒
RANDOMIZE_DOWNLOAD_DELAY = True # 随机延迟

# 4. 启用 Item Pipelines (按顺序执行)
ITEM_PIPELINES = {
    "Scrapy_Google.pipelines.FileProcessingPipeline": 50,     # 1. 生成基础信息
    "Scrapy_Google.pipelines.RedisDeduplicatePipeline": 100, # 2. URL 去重 (下载前过滤)
    "Scrapy_Google.pipelines.CustomGoogleFilesPipeline": 200, # 3. 文件下载
    "Scrapy_Google.pipelines.RedisMD5DeduplicatePipeline": 250,# 4. MD5 去重 (下载后根据内容过滤)
    "Scrapy_Google.pipelines.RedisStoragePipeline": 300,      # 5. 存储最终结果
}

# 5. 文件存储路径 (FILES_STORE)
# 你可以根据实际情况修改为绝对路径
FILES_STORE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'downloads')

# 5. Redis 配置 (对应原脚本 REDIS_HOST 等)
REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 6
REDIS_PREFIX = "crawler"

# 6. 语言检测与域名分类配置
DOMAIN_CONFIG_PATH = 'url_class_keywords.json'
LANGUAGE_MODEL_PATH = 'lid.176.bin'
LANGUAGE_CONFIDENCE_THRESHOLD = 0.8

# 6. Playwright 配置 (用于模拟浏览器处理 Google 反爬)
# 需要安装: pip install scrapy-playwright
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# 7. 日志设置
LOG_LEVEL = 'INFO'
LOG_ENCODING = 'utf-8'

FEED_EXPORT_ENCODING = "utf-8"
