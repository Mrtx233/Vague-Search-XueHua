import scrapy  # Scrapy 框架入口
import json  # 读取关键词 JSON 文件
import os  # 文件存在性检查等
import redis  # Redis 去重/状态存储
from urllib.parse import quote, urlparse  # URL 编码与域名解析
from Scrapy_Bing.items import BingFileItem  # Item 定义（下载与落库所需字段）
import re  # 用于从 URL 提取扩展名

class BingSpider(scrapy.Spider):  # Bing 搜索爬虫（通过搜索结果提取可下载文件链接）
    name = 'bing_spider'  # spider 名称（scrapy crawl 用）
    allowed_domains = ['bing.com']  # 允许请求的域名白名单（Scrapy 过滤用）

    def __init__(self, keyword_path=None, *args, **kwargs):  # 初始化 spider（支持命令行传入关键词路径）
        super(BingSpider, self).__init__(*args, **kwargs)  # 调用父类初始化
        self.keyword_path = keyword_path or r"D:\code_Python\Vague-Search-XueHua\json\output\泰语\人文地理.json"  # 关键词 JSON 文件路径（未传参时使用默认）
        self._kw_stats = {}  # 关键词统计：pages/items，用于日志与完成标记
        self._keyword_iter = None  # 关键词迭代器（用于严格串行切换关键词）

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):  # 通过 crawler 注入 settings/组件（Scrapy 标准扩展点）
        spider = super().from_crawler(crawler, *args, **kwargs)  # 交给 Scrapy 创建 spider 实例
        spider.rds = redis.Redis(  # 创建 Redis 客户端（用于关键词完成标记/去重依赖）
            host=crawler.settings['REDIS_HOST'],  # Redis host（settings 可覆盖）
            port=crawler.settings.getint('REDIS_PORT'),  # Redis 端口（settings 可覆盖）
            db=crawler.settings.getint('REDIS_DB'),  # Redis DB（settings 可覆盖）
            decode_responses=True  # 以 str 形式返回（避免 bytes 处理）
        )  # Redis 连接初始化结束
        spider.redis_prefix = crawler.settings['REDIS_PREFIX']  # Redis key 前缀（隔离不同项目/环境）
        return spider  # 返回配置完成的 spider

    def is_finished_bing(self, keyword):  # 判断关键词是否已完成（避免重复跑）
        """对应原脚本: bool(rds.sismember(f"{REDIS_PREFIX}:keyword_finished:bing", keyword))"""  # 说明 key 语义
        return self.rds.sismember(f"{self.redis_prefix}:keyword_finished:bing", keyword)  # Redis Set 判断成员是否存在

    def mark_finished_bing(self, keyword):  # 标记关键词完成（写 Redis，便于断点续跑）
        """对应原脚本: rds.sadd(f"{REDIS_PREFIX}:keyword_finished:bing", keyword)"""  # 说明 key 语义
        self.rds.sadd(f"{self.redis_prefix}:keyword_finished:bing", keyword)  # 将关键词加入完成集合
        s = self._kw_stats.get(keyword, {})  # 取该关键词统计信息
        pages = s.get("pages", 0)  # 已抓取页数
        items = s.get("items", 0)  # 已产出 item 数
        self.logger.info(f"关键词已处理完成并标记: {keyword} | pages={pages} | items={items}")  # 记录完成日志

    def _build_keyword_request(self, kw):  # 构造某个关键词的第 1 页请求
        search_query = f'"{kw}" filetype:xlsx'  # 搜索语句：精确匹配关键词 + 文件类型限制
        url = f"https://www.bing.com/search?q={quote(search_query)}"  # 拼接 Bing 搜索 URL（对 query 编码）
        self._kw_stats.setdefault(kw, {"pages": 0, "items": 0})  # 初始化该关键词的统计容器
        self.logger.info(f"开始关键词: {kw} | page=1")  # 记录开始日志（页码从 1）
        return scrapy.Request(  # 生成请求对象交给调度器
            url,  # 搜索页 URL
            callback=self.parse,  # 回调解析函数（async）
            meta={  # meta 用于在回调中传递上下 + Playwright 控制参数
                'keyword': kw,  # 当前关键词
                'playwright': True,  # 启用 scrapy-playwright 下载器
                'playwright_context': 'default',  # 复用默认浏览器上下文
                'page_no': 1,  # 当前页码（用于统计与翻页）
                'playwright_page_goto_kwargs': {
                    'wait_until': 'networkidle',  # 等待网络空闲后再返回响应
                    'timeout': 60000,             # 60秒超时
                }
            }  # meta 结束
        )  # Request 结束

    def _next_unfinished_keyword_request(self):  # 获取下一个未完成关键词的首个请求
        if self._keyword_iter is None:  # 迭代器未初始化时无法取下一个
            return None  # 返回空，表示没有可调度请求
        while True:  # 跳过已完成关键词，直到找到一个可跑的或耗尽
            try:  # 取下一个关键词
                kw = next(self._keyword_iter)  # 迭代器推进
            except StopIteration:  # 全部关键词处理完毕
                return None  # 没有下一个关键词
            if self.is_finished_bing(kw):  # 如果已完成，则跳过
                self.logger.info(f"跳过已完成关键词: {kw}")  # 记录跳过日志
                continue  # 继续取下一个关键词
            return self._build_keyword_request(kw)  # 找到未完成关键词则返回其第 1 页请求

    def start_requests(self):  # Scrapy 启动入口：为每个关键词发起首个搜索请求
        keywords = self.load_keywords(self.keyword_path)  # 从本地 JSON 加载关键词列表
        self._keyword_iter = iter(keywords)  # 初始化关键词迭代器（保证后续严格串行切换）
        first_req = self._next_unfinished_keyword_request()  # 取第一个可跑关键词的请求
        if first_req:  # 可能全部都已完成/为空
            yield first_req  # 仅调度一个关键词的第 1 页请求（严格串行）

    async def parse(self, response):  # 解析搜索结果页（Playwright 模式下支持 async）
        keyword = response.meta.get("keyword")  # 取出当前关键词
        page_no = int(response.meta.get("page_no") or 1)  # 取出并规范化页码（缺省为 1）
        self._kw_stats.setdefault(keyword, {"pages": 0, "items": 0})  # 确保统计容器存在
        if page_no > self._kw_stats[keyword]["pages"]:  # 记录最大页码（避免回退覆盖）
            self._kw_stats[keyword]["pages"] = page_no  # 更新已处理页数
        self.logger.info(f"解析关键词: {keyword} | page={page_no} | url={response.url}")  # 记录解析日志

        # ========== 调试：打印响应内容（前500字符） ==========
        response_preview = response.text[:500].replace('\n', ' ')
        self.logger.info(f"【响应内容预览】{response_preview}")

        results = response.xpath('//li[@class="b_algo"]')  # 定位 Bing 搜索结果条目

        # ========== 调试：打印所有 li 元素的 class ==========
        all_li_classes = response.xpath('//li/@class').getall()
        if page_no > 1:
            self.logger.info(f"【第{page_no}页】所有 li 的 class: {all_li_classes[:10]}")  # 只打印前10个

        # ========== 调试：检查是否有"无结果"提示 ==========
        if page_no > 1 and not results:
            no_results_text = response.xpath('//*[contains(text(), "没有找到") or contains(text(), "No results") or contains(text(), "Your search did not match")]/text()').getall()
            if no_results_text:
                self.logger.info(f"【第{page_no}页】检测到'无结果'提示: {no_results_text}")

        # ========== 调试：尝试其他可能的选择器 ==========
        if page_no > 1 and not results:
            # 尝试其他可能的搜索结果选择器
            selector_tests = [
                ('//li[contains(@class, "algo")]', 'algo'),
                ('//div[contains(@class, "b_result")]', 'b_result'),
                ('//div[contains(@class, "b_algo")]', 'div_b_algo'),
                ('//a[contains(@class, "title")]/ancestor::li', 'title_ancestor'),
            ]
            for selector, name in selector_tests:
                test_results = response.xpath(selector)
                if test_results:
                    self.logger.info(f"【第{page_no}页页】选择器 {name} 匹配到 {len(test_results)} 个结果")

        if not results:  # 如果没有任何结果
            self.logger.warning(f"关键词 '{keyword}' 未找到结果 | page={page_no}")  # 记录无结果告警
            self.mark_finished_bing(keyword)  # 标记该关键词已完成（避免重复跑）
            next_kw_req = self._next_unfinished_keyword_request()  # 切换到下一个关键词
            if next_kw_req:  # 若还有关键词可跑
                yield next_kw_req  # 调度下一个关键词的第 1 页
            return  # 结束该关键词处理

        extracted = 0  # 该页提取的 item 计数器
        for res in results:  # 遍历每条搜索结果
            item = BingFileItem()  # 创建一个 Item 容器（交给 pipelines 处理）

            item['url'] = res.xpath('.//h2/a/@href').get()  # 提取结果链接
            title_parts = res.xpath('.//h2/a//text()').getall()  # 提取标题文本片段
            item['title'] = "".join(title_parts).strip()  # 合并并清洗标题
            item['keyword'] = keyword  # 记录来源关键词

            if not item['url']:  # URL 为空则跳过
                continue  # 处理下一条结果

            clean_url = item['url'].split('?')[0].split('#')[0]  # 去掉 query/hash 以便提取扩展名
            ext_match = re.search(r'\.([a-zA-Z0-9]{1,10})$', clean_url)  # 从 URL 尾部匹配扩展名
            item['file_type'] = ext_match.group(1).lower() if ext_match else "xlsx"  # 无匹配时按默认 xlsx

            try:  # website 提取可能失败（URL 异常等）
                item['website'] = urlparse(item['url']).netloc  # 解析域名（host）
            except Exception:  # 解析失败兜底
                item['website'] = "unknown"  # 设置未知域名

            extracted += 1  # 增加页内计数
            yield item  # 产出 item（交由 pipelines 下载/去重/落库）

        self._kw_stats[keyword]["items"] += extracted  # 累加该关键词总 item 数
        self.logger.info(f"完成页面: {keyword} | page={page_no} | extracted={extracted}")  # 记录页完成日志

        next_page = response.xpath('//a[@title="下一页"]/@href').get() or response.xpath('//a[@title="Next page"]/@href').get()  # 兼容中文/英文“下一页”链接
        if next_page:  # 若存在下一页
            meta = dict(response.meta)  # 复制 meta（避免原对象被共享修改）
            meta["page_no"] = page_no + 1  # 页码 +1
            yield response.follow(next_page, callback=self.parse, meta=meta)  # 跟进下一页继续解析
        else:  # 没有下一页则认为关键词抓取结束
            self.mark_finished_bing(keyword)  # 标记该关键词完成
            next_kw_req = self._next_unfinished_keyword_request()  # 切换到下一个关键词
            if next_kw_req:  # 若还有关键词可跑
                yield next_kw_req  # 调度下一个关键词的第 1 页

    def load_keywords(self, path):  # 从 JSON 文件加载关键词列表
        """  # docstring 起始
        从本地 JSON 文件加载关键词，并过滤掉空值  # 功能描述
        """  # docstring 结束
        try:  # 捕获读文件/解析 JSON 异常
            if not os.path.exists(path):  # 路径不存在直接返回空列表
                self.logger.error(f"关键词文件不存在: {path}")  # 输出错误日志
                return []  # 返回空关键词列表
            with open(path, 'r', encoding='utf-8-sig') as f:  # 以 utf-8-sig 兼容带 BOM 文件
                data = json.load(f)  # 解析 JSON 为 Python 对象
                return [item['中文'] for item in data if item.get('外文')]  # 取字段“外文”并过滤空值
        except Exception as e:  # 兜底异常处理
            self.logger.error(f"加载关键词异常: {e}")  # 输出异常原因
            return []  # 异常时返回空列表
