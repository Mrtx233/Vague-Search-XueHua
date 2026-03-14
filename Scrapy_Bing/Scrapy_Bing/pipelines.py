import redis
import json
import time
import os
from scrapy.exceptions import DropItem
from scrapy.pipelines.files import FilesPipeline
from Scrapy_Bing.utils import SnowflakeIdGenerator, calculate_file_md5
from Scrapy_Bing.utils.domain_classifier import DomainClassifier
from Scrapy_Bing.utils.language_detector import LanguageDetector

# -------------------------- Pipeline 0: 自定义文件下载与存储 --------------------------
class CustomBingFilesPipeline(FilesPipeline):
    """
    自定义文件存储路径逻辑: {雪花ID}/master/{MD5}.xlsx
    """
    def get_media_requests(self, item, info):
        from scrapy import Request
        yield Request(item['url'], meta={'item': item})

    def file_path(self, request, response=None, info=None, *, item=None):
        item = request.meta.get('item')
        snowflake_id = item.get('snowflake_id', 'unknown')
        file_ext = item.get('file_type', 'xlsx')
        
        filename = os.path.basename(request.url).split('?')[0]
        if not filename.endswith(f".{file_ext}"):
            filename = f"temp_{int(time.time())}.{file_ext}"
            
        return f"{snowflake_id}/master/{filename}"

    def item_completed(self, results, item, info):
        # 下载完成后的处理
        if results:
            success, file_info = results[0]
            if success:
                md5 = file_info['checksum']
                item['file_hash'] = md5
                
                # 获取下载后的原始绝对路径
                files_store = info.spider.settings.get('FILES_STORE')
                old_rel_path = file_info['path']
                old_abs_path = os.path.join(files_store, old_rel_path)
                
                # 构造目标绝对路径: {snowflake_id}/master/{MD5}.xlsx
                snowflake_id = item.get('snowflake_id', 'unknown')
                file_ext = item.get('file_type', 'xlsx')
                new_rel_path = f"{snowflake_id}/master/{md5}.{file_ext}"
                new_abs_path = os.path.join(files_store, new_rel_path)
                
                # 执行物理重命名 (Move)
                try:
                    if os.path.exists(old_abs_path):
                        import shutil
                        # 确保目标目录存在
                        os.makedirs(os.path.dirname(new_abs_path), exist_ok=True)
                        shutil.move(old_abs_path, new_abs_path)
                        item['local_path'] = new_rel_path
                        info.spider.logger.info(f"文件已重命名为 MD5: {new_rel_path}")
                except Exception as e:
                    info.spider.logger.error(f"文件重命名失败: {e}")
                    
        return item

# -------------------------- Pipeline 1: URL 去重 --------------------------
class RedisDeduplicatePipeline:
    """
    使用 Redis 进行 URL 全局去重
    """
    def open_spider(self, spider):
        self.rds = redis.Redis(
            host=spider.settings['REDIS_HOST'],
            port=spider.settings.getint('REDIS_PORT'),
            db=spider.settings.getint('REDIS_DB'),
            decode_responses=True
        )
        self.prefix = spider.settings['REDIS_PREFIX']

    def process_item(self, item, spider):
        seen_key = f"{self.prefix}:seen_url"
        if not self.rds.sadd(seen_key, item['url']):
            spider.logger.info(f"URL 已存在，跳过: {item['url'][:50]}")
            raise DropItem(f"Duplicate URL: {item['url']}")
        return item

# -------------------------- Pipeline 2: 核心业务处理 --------------------------
class FileProcessingPipeline:
    """
    处理业务逻辑：生成 ID、语种检测、域名分类
    """
    def open_spider(self, spider):
        self.snowflake = SnowflakeIdGenerator()
        
        domain_config = spider.settings.get('DOMAIN_CONFIG_PATH', 'url_class_keywords.json')
        lang_model = spider.settings.get('LANGUAGE_MODEL_PATH', 'lid.176.bin')
        lang_threshold = spider.settings.get('LANGUAGE_CONFIDENCE_THRESHOLD', 0.8)
        
        self.domain_classifier = DomainClassifier(domain_config)
        self.language_detector = LanguageDetector(lang_model, lang_threshold)

    def process_item(self, item, spider):
        item['snowflake_id'] = self.snowflake.generate()
        item['crawl_time'] = int(time.time() * 1000)
        
        # 语种检测
        item['language'] = self.language_detector.detect_with_threshold_zh(item['title'])
        
        # 域名分类
        domain_result = self.domain_classifier.classify_url(item['url'])
        item['domain_class'] = domain_result.get("domain_class", "")
        
        return item

# -------------------------- Pipeline 3: 最终存储 --------------------------
class RedisStoragePipeline:
    """
    将抓取结果写入 Redis 队列，并保存本地元数据
    """
    def open_spider(self, spider):
        self.rds = redis.Redis(
            host=spider.settings['REDIS_HOST'],
            port=spider.settings.getint('REDIS_PORT'),
            db=spider.settings.getint('REDIS_DB'),
            decode_responses=True
        )
        self.prefix = spider.settings['REDIS_PREFIX']

    def process_item(self, item, spider):
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
        
        files_store = spider.settings.get('FILES_STORE')
        if files_store and item.get('snowflake_id') and item.get('file_hash'):
            meta_dir = os.path.join(files_store, item['snowflake_id'], "meta")
            os.makedirs(meta_dir, exist_ok=True)
            
            meta_path = os.path.join(meta_dir, f"{item['file_hash']}.json")
            with open(meta_path, 'w', encoding='utf-8') as f:
                json.dump(result_json, f, ensure_ascii=False, indent=2)
            spider.logger.info(f"元数据已保存到本地: {meta_path}")

        result_key = f"{self.prefix}:results"
        self.rds.rpush(result_key, json.dumps(result_json, ensure_ascii=False))
        
        spider.logger.info(f"成功保存结果到 Redis: {item['snowflake_id']} | {item['title'][:20]}")
        return item

# -------------------------- Pipeline 4: MD5 级别去重 --------------------------
class RedisMD5DeduplicatePipeline:
    """
    使用 Redis 对文件内容 (MD5) 进行全局去重
    """
    def open_spider(self, spider):
        self.rds = redis.Redis(
            host=spider.settings['REDIS_HOST'],
            port=spider.settings.getint('REDIS_PORT'),
            db=spider.settings.getint('REDIS_DB'),
            decode_responses=True
        )
        self.prefix = spider.settings['REDIS_PREFIX']

    def process_item(self, item, spider):
        file_hash = item.get('file_hash')
        if not file_hash:
            return item
        
        md5_key = f"{self.prefix}:seen_md5"
        
        if not self.rds.sadd(md5_key, file_hash):
            spider.logger.info(f"MD5 已存在，跳过记录: {file_hash}")
            
            local_path = item.get('local_path')
            files_store = spider.settings.get('FILES_STORE')
            if local_path and files_store:
                full_path = os.path.join(files_store, local_path)
                if os.path.exists(full_path):
                    os.remove(full_path)
                    spider.logger.info(f"已删除内容重复的文件: {full_path}")
            
            raise DropItem(f"Duplicate MD5: {file_hash}")
        
        return item
