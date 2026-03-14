# Scrapy settings for Scrapy_Bing project
import os

BOT_NAME = "Scrapy_Bing"

SPIDER_MODULES = ["Scrapy_Bing.spiders"]
NEWSPIDER_MODULE = "Scrapy_Bing.spiders"

# 1. 设置 User-Agent (使用更真实的现代浏览器 UA)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'

# 2. 遵守 robots.txt
ROBOTSTXT_OBEY = False

# 3. 配置并发与延迟 (模拟真人低频操作)
CONCURRENT_REQUESTS = 1
DOWNLOAD_DELAY = 10  # 基础延迟 10 秒
RANDOMIZE_DOWNLOAD_DELAY = True # 随机延迟

# 4. 启用 Item Pipelines (按顺序执行)
ITEM_PIPELINES = {
    "Scrapy_Bing.pipelines.FileProcessingPipeline": 50,     # 1. 生成基础信息
    "Scrapy_Bing.pipelines.RedisDeduplicatePipeline": 100, # 2. URL 去重
    "Scrapy_Bing.pipelines.CustomBingFilesPipeline": 200,  # 3. 文件下载
    "Scrapy_Bing.pipelines.RedisMD5DeduplicatePipeline": 250,# 4. MD5 去重
    "Scrapy_Bing.pipelines.RedisStoragePipeline": 300,      # 5. 存储结果
}

# 5. Redis 配置
REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 2
REDIS_PREFIX = "crawler"

# 6. 语言检测与域名分类配置
DOMAIN_CONFIG_PATH = 'url_class_keywords.json'
LANGUAGE_MODEL_PATH = 'lid.176.bin'
LANGUAGE_CONFIDENCE_THRESHOLD = 0.8

# 7. Playwright 配置
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# 8. Playwright 持久化上下文 (保持同一个浏览器窗口/配置文件)
PLAYWRIGHT_CONTEXTS = {
    "default": {
        "user_data_dir": os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "pw_profile"),
        "headless": False,
    }
}

PLAYWRIGHT_MAX_PAGES_PER_CONTEXT = 1

# 8. Playwright 隐身模式与回调配置
def run_stealth(page, request):
    from playwright_stealth import stealth_sync
    stealth_sync(page)

PLAYWRIGHT_PROCESS_REQUEST_KWARGS = {
    "page_init_callback": run_stealth,
}

# 9. 超时设置
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 120000 # 120 秒

# 10. 文件存储路径
FILES_STORE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'downloads')

# 11. 日志设置
LOG_LEVEL = 'INFO'
LOG_ENCODING = 'utf-8'

FEED_EXPORT_ENCODING = "utf-8"
