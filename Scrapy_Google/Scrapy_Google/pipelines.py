# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


import redis
import json
import time
import os
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline
from Scrapy_Google.utils import SnowflakeIdGenerator, calculate_file_md5
from Scrapy_Google.utils.domain_classifier import DomainClassifier
from Scrapy_Google.utils.language_detector import LanguageDetector

# -------------------------- Pipeline 0: 自定义文件下载与存储 --------------------------
class CustomGoogleFilesPipeline(FilesPipeline):
    """
    自定义文件存储路径逻辑: {雪花ID}/master/{MD5}.xlsx
    """
    def get_media_requests(self, item, info):
        # 触发下载请求，将 URL 传递给 Scrapy 引擎
        from scrapy import Request
        yield Request(item['url'], meta={'item': item})

    def file_path(self, request, response=None, info=None, *, item=None):
        # 从 meta 中取回 item
        item = request.meta.get('item')
        snowflake_id = item.get('snowflake_id', 'unknown')
        # 获取文件名 (优先从 item['file_hash'] 获取，如果没有则暂时使用随机或原始名)
        # 注意：这里如果 MD5 还没生成，可以先用随机名，下载完在后续 Pipeline 改名
        # 但 Scrapy FilesPipeline 默认会在下载完成后才算 MD5
        # 为了严格复刻原脚本，我们这里构造路径
        file_ext = item.get('file_type', 'xlsx')
        
        # 初始路径可以先按雪花ID分目录
        # {snowflake_id}/master/{original_filename_or_hash}
        filename = os.path.basename(request.url).split('?')[0]
        if not filename.endswith(f".{file_ext}"):
            filename = f"temp_{int(time.time())}.{file_ext}"
            
        return f"{snowflake_id}/master/{filename}"

    def item_completed(self, results, item, info):
        # 下载完成后的处理
        # results 是一个元组列表: [(success, file_info), ...]
        if results:
            success, file_info = results[0]
            if success:
                # 获取 Scrapy 自动生成的 MD5 (checksum)
                item['file_hash'] = file_info['checksum']
                item['local_path'] = file_info['path']
                
                # 如果需要重命名为 MD5.xlsx，可以在这里执行物理重命名
                # 或者直接在后续 Pipeline 中记录这个 path
                spider = info.spider
                spider.logger.info(f"文件下载成功: {item['local_path']}")
        return item

# -------------------------- Pipeline 1: URL 去重 --------------------------
class RedisDeduplicatePipeline:
    """
    使用 Redis 进行 URL 全局去重 (对应原脚本 is_new_url)
    """
    def open_spider(self, spider):
        # 初始化 Redis 连接 (从 settings 获取配置)
        self.rds = redis.Redis(
            host=spider.settings.get('REDIS_HOST', '10.229.32.166'),
            port=spider.settings.get('REDIS_PORT', 6379),
            db=spider.settings.get('REDIS_DB', 6),
            decode_responses=True
        )
        self.prefix = spider.settings.get('REDIS_PREFIX', 'crawler')

    def process_item(self, item, spider):
        # 构造 Redis 去重 Key
        seen_key = f"{self.prefix}:seen_url"
        
        # 尝试插入 URL 集合，sadd 返回 1 表示首次出现
        if not self.rds.sadd(seen_key, item['url']):
            spider.logger.info(f"URL 已存在，跳过: {item['url'][:50]}")
            raise DropItem(f"Duplicate URL: {item['url']}")
        
        return item

# -------------------------- Pipeline 2: 核心业务处理 --------------------------
class FileProcessingPipeline:
    """
    处理业务逻辑：生成 ID、语种检测、域名分类 (对应原脚本 handle_download_task_async)
    """
    def open_spider(self, spider):
        self.snowflake = SnowflakeIdGenerator()
        
        # 从 settings 获取配置路径
        domain_config = spider.settings.get('DOMAIN_CONFIG_PATH', 'url_class_keywords.json')
        lang_model = spider.settings.get('LANGUAGE_MODEL_PATH', 'lid.176.bin')
        lang_threshold = spider.settings.get('LANGUAGE_CONFIDENCE_THRESHOLD', 0.8)
        
        self.domain_classifier = DomainClassifier(domain_config)
        self.language_detector = LanguageDetector(lang_model, lang_threshold)

    def process_item(self, item, spider):
        # 1. 生成唯一雪花 ID
        item['snowflake_id'] = self.snowflake.generate()
        
        # 2. 获取当前采集时间
        item['crawl_time'] = int(time.time() * 1000)
        
        # 3. 语种检测 (根据标题检测)
        item['language'] = self.language_detector.detect_with_threshold_zh(item['title'])
        
        # 4. 域名分类
        domain_result = self.domain_classifier.classify_url(item['url'])
        item['domain_class'] = domain_result.get("domain_class", "")
        
        return item

# -------------------------- Pipeline 3: 最终存储 --------------------------
class RedisStoragePipeline:
    """
    将抓取结果写入 Redis 队列 (对应原脚本 push_jsonl_line)
    """
    def open_spider(self, spider):
        self.rds = redis.Redis(
            host=spider.settings.get('REDIS_HOST', '10.229.32.166'),
            port=spider.settings.get('REDIS_PORT', 6379),
            db=spider.settings.get('REDIS_DB', 6),
            decode_responses=True
        )
        self.prefix = spider.settings.get('REDIS_PREFIX', 'crawler')

    def process_item(self, item, spider):
        # 1. 构造保存结果的 JSON (元数据)
        result_json = {
            "webSite": item['website'] or "unknown",
            "crawlTime": item['crawl_time'],
            "srcUrl": item['url'],
            "title": item['title'],
            "hash": item['file_hash'],
            "extend": {
                "keyword": item['keyword'],
                "language": item['language'],
                "doMain": item['domain_class'],
                "type": item['file_type']
            }
        }
        
        # 2. 保存到本地 meta 目录 (复刻原脚本逻辑)
        # 路径: {FILES_STORE}/{snowflake_id}/meta/{md5}.json
        files_store = spider.settings.get('FILES_STORE')
        if files_store and item.get('snowflake_id') and item.get('file_hash'):
            meta_dir = os.path.join(files_store, item['snowflake_id'], "meta")
            os.makedirs(meta_dir, exist_ok=True)
            
            meta_path = os.path.join(meta_dir, f"{item['file_hash']}.json")
            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(result_json, f, ensure_ascii=False, indent=2)
            spider.logger.info(f"元数据已保存到本地: {meta_path}")

        # 3. 写入 Redis 结果队列
        result_key = f"{self.prefix}:results"
        self.rds.rpush(result_key, json.dumps(result_json, ensure_ascii=False))
        
        spider.logger.info(f"成功保存结果到 Redis: {item['snowflake_id']} | {item['title'][:20]}")
        return item

# -------------------------- Pipeline 4: MD5 级别去重 --------------------------
class RedisMD5DeduplicatePipeline:
    """
    使用 Redis 对文件内容 (MD5) 进行全局去重
    (即使 URL 不同，内容相同也跳过)
    """
    def open_spider(self, spider):
        self.rds = redis.Redis(
            host=spider.settings.get('REDIS_HOST', '10.229.32.166'),
            port=spider.settings.get('REDIS_PORT', 6379),
            db=spider.settings.get('REDIS_DB', 6),
            decode_responses=True
        )
        self.prefix = spider.settings.get('REDIS_PREFIX', 'crawler')

    def process_item(self, item, spider):
        # 只有下载成功拿到 MD5 之后才能去重
        file_hash = item.get('file_hash')
        if not file_hash:
            return item
        
        # 构造 Redis 去重 Key
        md5_key = f"{self.prefix}:seen_md5"
        
        # 尝试插入 MD5 集合，sadd 返回 1 表示首次出现
        if not self.rds.sadd(md5_key, file_hash):
            spider.logger.info(f"MD5 已存在，跳过记录: {file_hash}")
            
            # 如果 MD5 已存在，通常我们会删除刚下载的重复文件以节省空间
            local_path = item.get('local_path')
            files_store = spider.settings.get('FILES_STORE')
            if local_path and files_store:
                full_path = os.path.join(files_store, local_path)
                if os.path.exists(full_path):
                    os.remove(full_path)
                    spider.logger.info(f"已删除内容重复的文件: {full_path}")
            
            raise DropItem(f"Duplicate MD5: {file_hash}")
        
        return item
