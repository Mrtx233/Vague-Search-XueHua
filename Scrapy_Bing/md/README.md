# Scrapy_Bing 项目深度解析与使用指南

本文档旨在为初学者详细解析 **Scrapy_Bing** 项目的核心原理、工作流程及使用方法。本项目是一个基于 **Scrapy + Playwright + Redis** 的分布式爬虫，专门用于从 Bing 搜索引擎批量抓取特定关键词下的 `.xlsx` 文件。

---

## 1. 项目概览

### 1.1 核心功能
- **关键词搜索**：读取本地 JSON 文件中的关键词，在 Bing 搜索 `"{关键词}" filetype:xlsx`。
- **浏览器模拟**：使用 Playwright 驱动真实浏览器（Chromium），规避简单的静态反爬。
- **文件下载**：自动提取搜索结果中的文件链接并下载。
- **智能去重**：
  - **URL 级去重**：不重复抓取同一个链接。
  - **内容级去重**：下载后计算 MD5，如果文件内容已存在则删除重复文件。
- **数据清洗与落库**：自动识别语种、分类域名（政府/教育/商业等），并将元数据存入 Redis 和本地 JSON。

### 1.2 关键技术栈
- **Scrapy**：Python 最强大的爬虫框架，负责调度、并发控制、数据流转。
- **scrapy-playwright**：Scrapy 的插件，用于对接 Playwright 浏览器，解决动态渲染和 JS 逆向问题。
- **Redis**：用于存储去重指纹（Set）和结果队列（List），支持分布式扩展。
- **FastText**：Facebook 开源的文本分类库，用于语种识别（依赖 `lid.176.bin` 模型）。

---

## 2. 核心模块深度解析

### 2.1 爬虫主体 (Spider)
**文件位置**：[bing_spider.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/spiders/bing_spider.py)

Spider 是爬虫的大脑，负责“去哪抓”和“怎么解析”。

- **启动流程 (`start_requests`)**：
  1. 读取关键词文件（JSON 格式）。
  2. **串行调度**：每次只取一个关键词，构造 Bing 搜索 URL。
  3. 发起请求时启用 Playwright (`meta={'playwright': True}`)，模拟浏览器访问。

- **解析流程 (`parse`)**：
  1. **提取结果**：使用 XPath `//li[@class="b_algo"]` 定位搜索结果条目。
  2. **提取字段**：从条目中解析出 URL、标题。
  3. **翻页逻辑**：
     - 提取“下一页”链接。
     - 如果有下一页，继续请求（递归调用 `parse`）。
     - 如果无下一页或无结果，**自动切换下一个关键词**（`_next_unfinished_keyword_request`）。

- **反爬处理**：
  - 遇到 Bing 验证码（"系统检测到异常流量"）时，打印告警日志（原先有重试逻辑，现已简化为记录并跳过或由人工干预）。

### 2.2 数据管道 (Pipelines)
**文件位置**：[pipelines.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/pipelines.py)

Pipeline 是数据流水线，Item（抓取结果）会依次经过以下 5 个关卡：

1. **`FileProcessingPipeline` (优先级 50)**
   - **身份生成**：生成全局唯一的雪花 ID (`snowflake_id`)。
   - **语种检测**：分析标题是中文、英文还是泰语等。
   - **域名分类**：判断来源是 `.gov` (政府)、`.edu` (教育) 还是其他。

2. **`RedisDeduplicatePipeline` (优先级 100)**
   - **URL 去重**：查询 Redis 集合 `crawler:seen_url`。
   - 如果 URL 已存在，直接丢弃 (`DropItem`)，不再进行后续下载。

3. **`CustomBingFilesPipeline` (优先级 200)**
   - **文件下载**：继承自 Scrapy `FilesPipeline`，自动发起文件下载请求。
   - **重命名**：默认下载到 `downloads/` 目录。
   - **路径规则**：下载完成后，将文件重命名为 `{snowflake_id}/master/{MD5}.xlsx`。

4. **`RedisMD5DeduplicatePipeline` (优先级 250)**
   - **内容去重**：计算文件 MD5，查询 Redis 集合 `crawler:seen_md5`。
   - 如果内容重复（即使 URL 不同），删除刚下载的本地文件并丢弃 Item，节省磁盘空间。

5. **`RedisStoragePipeline` (优先级 300)**
   - **结果存储**：
     - **Redis**：将完整元数据 JSON 推入 `crawler:results` 队列，供下游业务消费。
     - **本地**：在 `downloads/{snowflake_id}/meta/` 下保存一份 JSON 元数据。

### 2.3 配置中心 (Settings)
**文件位置**：[settings.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/settings.py)

所有“硬编码”配置都集中在这里，修改后全局生效：

- **Redis 配置**：HOST, PORT, DB (默认 2), PREFIX。
- **Playwright**：浏览器路径、是否无头模式 (`headless`)、超时时间。
- **文件路径**：下载目录 (`downloads`)、关键词文件路径、模型路径。
- **并发控制**：`CONCURRENT_REQUESTS=1`，`DOWNLOAD_DELAY=10`（慢速爬取以规避封禁）。

---

## 3. 快速开始指南

### 3.1 环境准备
1. **Python 环境**：确保安装 Python 3.8+。
2. **依赖安装**：
   ```bash
   pip install scrapy scrapy-playwright redis fasttext tldextract
   playwright install  # 安装浏览器驱动
   ```
3. **Redis 服务**：确保本地或远程 Redis 服务可用，并在 `settings.py` 中配置 IP 和端口。
4. **模型文件**：确保项目根目录下有 `lid.176.bin`（FastText 语言模型）和 `url_class_keywords.json`。

### 3.2 运行爬虫
在项目根目录 (`Scrapy_Bing/`) 下打开终端：

**方式一：使用默认配置运行**
```powershell
scrapy crawl bing_spider
```

**方式二：指定关键词文件运行**
```powershell
scrapy crawl bing_spider -a keyword_path="D:\path\to\your\keywords.json"
```

### 3.3 关键词文件格式
JSON 文件需满足以下格式（必须包含 `外文` 字段）：
```json
[
  { "外文": "machine learning xlsx" },
  { "外文": "financial report 2023" }
]
```

---

## 4. 常见问题与维护

### Q1: 如何查看抓取进度？
- **日志**：控制台会输出 `[bing_spider] INFO: 解析关键词: xxx | page=1 | extracted=10`。
- **Redis**：
  - 查看已完成关键词：`SMEMBERS crawler:keyword_finished:bing`
  - 查看已抓取 URL 数：`SCARD crawler:seen_url`
  - 查看抓取结果队列：`LLEN crawler:results`

### Q2: 爬虫被 Bing 封禁了怎么办？
- 日志会出现 `⚠️ 拦截：关键词 'xxx' 触发验证码！`。
- **解决方法**：
  1. 暂停爬虫。
  2. 调大 `DOWNLOAD_DELAY`（如设为 30 秒）。
  3. 手动在浏览器访问 Bing 完成验证码验证（如果是 IP 封禁则需更换 IP 代理）。

### Q3: 为什么 Redis 里没有数据？
- 检查 `settings.py` 里的 `REDIS_HOST` 和 `REDIS_DB` 是否正确。
- 只有成功下载并去重后的文件才会写入 Redis `results` 队列。

### Q4: 如何清理历史数据重新跑？
- **清空 Redis**（慎用）：
  ```bash
  redis-cli -n 2 FLUSHDB
  ```
- **删除本地文件**：删除 `downloads/` 目录下的内容。

---

## 5. 目录结构说明
```text
Scrapy_Bing/
├── scrapy.cfg               # Scrapy 项目入口
├── url_class_keywords.json  # 域名分类规则
├── lid.176.bin              # 语种识别模型
├── downloads/               # [自动生成] 结果下载目录
├── pw_profile/              # [自动生成] 浏览器用户数据
└── Scrapy_Bing/             # 代码包
    ├── spiders/
    │   └── bing_spider.py   # 核心爬虫代码
    ├── settings.py          # 全局配置
    ├── pipelines.py         # 数据处理管道
    ├── items.py             # 数据结构定义
    └── utils/               # 工具类 (雪花ID, 语言检测等)
```
