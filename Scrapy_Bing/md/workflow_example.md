# 一个关键词的全流程运行模拟：从搜索到落库

本文档以关键词 **"machine learning"** 为例，详细拆解从发起请求到最终入库的每一步，并指出代码中对应的关键方法。

---

## 1. 发起搜索 (Spider 侧)
**阶段目标**：构造 Bing 搜索 URL，模拟浏览器访问。

1. **读取关键词**：
   - `start_requests` 调用 `load_keywords`，从 JSON 中读到 `{"外文": "machine learning"}`。
   - 代码：`load_keywords` ([bing_spider.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/spiders/bing_spider.py))

2. **构造请求**：
   - 调用 `_build_keyword_request("machine learning")`。
   - 拼装搜索词：`"machine learning" filetype:xlsx`。
   - 编码 URL：`https://www.bing.com/search?q=%22machine%20learning%22%20filetype%3Axlsx`。
   - 生成 `scrapy.Request`，并在 `meta` 中设置 `playwright=True`（启用浏览器渲染）。
   - 代码：`_build_keyword_request` ([bing_spider.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/spiders/bing_spider.py))

3. **发送请求**：
   - Scrapy 调度器将请求交给 `scrapy-playwright` 下载器。
   - 下载器启动 Chromium 浏览器，加载 Bing 搜索页，渲染完成后返回 `Response` 对象。

---

## 2. 解析页面 (Spider 侧)
**阶段目标**：从 Bing 搜索结果中提取文件链接，生成 Item。

1. **解析回调**：
   - 响应返回后，进入 `parse(response)` 方法。

2. **提取条目**：
   - 使用 XPath `//li[@class="b_algo"]` 找到搜索结果列表。
   - 假设第一条结果是斯坦福大学的课程表：
     - **URL**: `https://cs229.stanford.edu/schedule.xlsx`
     - **Title**: `CS229: Machine Learning Schedule`

3. **生成 Item**：
   - 创建 `BingFileItem` 对象，填充 `url`、`title`、`keyword` ("machine learning")。
   - 解析文件类型：从 URL 后缀提取出 `xlsx`。
   - 解析域名：从 URL 提取出 `cs229.stanford.edu`。
   - 代码：`parse` ([bing_spider.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/spiders/bing_spider.py))

4. **提交 Item**：
   - `yield item`，将该条数据交给 Pipelines 处理。

5. **翻页（如有）**：
   - 提取“下一页”链接，递归调用 `yield response.follow(...)` 继续抓第 2 页。

---

## 3. 数据处理 (Pipelines 侧)
**阶段目标**：数据清洗、去重、下载、落库。Item 会按顺序经过 5 个管道。

### 3.1 身份与分类 (`FileProcessingPipeline`)
- **生成 ID**：调用 `SnowflakeIdGenerator` 生成唯一 ID，例如 `187654321012345`。
- **语种检测**：调用 `LanguageDetector` 分析标题 "CS229: Machine Learning Schedule"，识别为 `en` (英语)。
- **域名分类**：调用 `DomainClassifier` 分析 `stanford.edu`，识别为 `edu` (教育机构)。
- 代码：`FileProcessingPipeline.process_item` ([pipelines.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/pipelines.py))

### 3.2 URL 去重 (`RedisDeduplicatePipeline`)
- **查重**：向 Redis 发送指令 `SADD crawler:seen_url "https://cs229.stanford.edu/schedule.xlsx"`。
- **判断**：
  - 返回 `1`（新 URL）：继续往下走。
  - 返回 `0`（已存在）：抛出 `DropItem` 异常，**流程终止**（不下载）。
- 代码：`RedisDeduplicatePipeline.process_item` ([pipelines.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/pipelines.py))

### 3.3 文件下载 (`CustomBingFilesPipeline`)
- **发起下载**：
  - `get_media_requests` 方法被调用，对 `https://cs229.stanford.edu/schedule.xlsx` 发起一个新的 `scrapy.Request`。
- **保存文件**：
  - 下载成功后，文件暂时保存在 `downloads/187654321012345/master/temp_timestamp.xlsx`。
- **计算哈希**：
  - Scrapy 计算文件内容的 MD5，假设为 `a1b2c3d4e5f6...`。
- **重命名**：
  - `item_completed` 方法将文件重命名为：`downloads/187654321012345/master/a1b2c3d4e5f6.xlsx`。
  - 将相对路径写入 `item['local_path']`。
- 代码：`CustomBingFilesPipeline` ([pipelines.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/pipelines.py))

### 3.4 内容去重 (`RedisMD5DeduplicatePipeline`)
- **查重**：向 Redis 发送指令 `SADD crawler:seen_md5 "a1b2c3d4e5f6..."`。
- **判断**：
  - 返回 `1`（新内容）：继续。
  - 返回 `0`（内容重复）：说明虽然 URL 不一样，但这文件以前下过。
    - **删除**：立即删除刚下载的 `a1b2c3d4e5f6.xlsx` 文件。
    - **丢弃**：抛出 `DropItem`，流程终止。
- 代码：`RedisMD5DeduplicatePipeline.process_item` ([pipelines.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/pipelines.py))

### 3.5 结果落库 (`RedisStoragePipeline`)
- **构造最终 JSON**：
  ```json
  {
    "webSite": "cs229.stanford.edu",
    "srcUrl": "https://cs229.stanford.edu/schedule.xlsx",
    "title": "CS229: Machine Learning Schedule",
    "hash": "a1b2c3d4e5f6...",
    "extend": {
      "keyword": "machine learning",
      "language": "en",
      "doMain": "edu",
      "type": "xlsx"
    }
  }
  ```
- **写本地**：保存到 `downloads/187654321012345/meta/a1b2c3d4e5f6.json`。
- **写 Redis**：执行 `RPUSH crawler:results {...json_str...}`，供下游程序消费。
- 代码：`RedisStoragePipeline.process_item` ([pipelines.py](file:///d:/code_Python/Vague-Search-XueHua/Scrapy_Bing/Scrapy_Bing/pipelines.py))

---

## 4. 流程结束
当 "machine learning" 的所有页都翻完，或者遇到无结果页面时：
1. 调用 `mark_finished_bing("machine learning")`，在 Redis `crawler:keyword_finished:bing` 集合中记录该关键词。
2. 调用 `_next_unfinished_keyword_request`，自动取出下一个关键词（如 "financial report"），重复上述所有步骤。
