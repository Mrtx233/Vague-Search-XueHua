import os

BOT_NAME = "Scrapy_Google"

SPIDER_MODULES = ["Scrapy_Google.spiders"]
NEWSPIDER_MODULE = "Scrapy_Google.spiders"

USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
ROBOTSTXT_OBEY = False

CONCURRENT_REQUESTS = 1
DOWNLOAD_DELAY = 10
RANDOMIZE_DOWNLOAD_DELAY = True

ITEM_PIPELINES = {
    "Scrapy_Google.pipelines.FileProcessingPipeline": 50,
    "Scrapy_Google.pipelines.RedisDeduplicatePipeline": 100,
    "Scrapy_Google.pipelines.CustomGoogleFilesPipeline": 200,
    "Scrapy_Google.pipelines.RedisMD5DeduplicatePipeline": 250,
    "Scrapy_Google.pipelines.RedisStoragePipeline": 300,
}

FILES_STORE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'downloads')

REDIS_HOST = "10.229.32.166"
REDIS_PORT = 6379
REDIS_DB = 2
REDIS_PREFIX = "crawler"

DOMAIN_CONFIG_PATH = 'url_class_keywords.json'
LANGUAGE_MODEL_PATH = 'lid.176.bin'
LANGUAGE_CONFIDENCE_THRESHOLD = 0.8

DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"


def run_stealth(page, request):
    from playwright_stealth import stealth_sync
    stealth_sync(page)


PLAYWRIGHT_PROCESS_REQUEST_KWARGS = {
    "page_init_callback": run_stealth,
}

LOG_LEVEL = 'INFO'
LOG_ENCODING = 'utf-8'

PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 120000

FEED_EXPORT_ENCODING = "utf-8"
