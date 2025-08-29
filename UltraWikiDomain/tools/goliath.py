import json
import time
import requests
import logging
import base64
import random
import os
from typing import Dict, Any, Optional

# 默认配置（供其他模块直接引用，不再强制依赖环境变量）
DEFAULT_GOLIATH_ADDR = "https://rockmgrgo.wenxiaobai.com/rockopenai/v1/web_search"
DEFAULT_GOLIATH_TOKEN = "rock-ShWAczRU3K8Pk12p6NQalR3JP1GD7pUhemTwNnmD7PW26"  # 私有环境使用
DEFAULT_GOLIATH_TIMEOUT_MS = 120000
DEFAULT_GOLIATH_MAX_RETRY = 2
DEFAULT_GOLIATH_BIZ_DEF = "ref_fetch"


# 设置日志
logger = logging.getLogger('GoliathTool')
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)


class GoliathTool:
    """Goliath网页爬取工具"""
    
    def __init__(
        self,
        addr: str,
        rock_token: str,
        timeout_ms: int = 120000,
        max_retry: int = 2,
        biz_def: str = "test",
    ):
        self.addr = addr
        self.rock_token = rock_token
        self.timeout_ms = timeout_ms
        self.max_retry = max_retry
        self.biz_def = biz_def
        self.model_name = "yuanshi/goliath/retrieve"

    def retrieve(
        self,
        url: str,
        retrieve_type: str = "MARKDOWN_RICH",
    ) -> Dict[str, Any]:
        """
        爬取和解析网页内容
        
        Args:
            url: 要爬取的网址
            retrieve_type: 爬取类型 (RAW, TEXT, MARKDOWN, IMAGE, PDF, VIDEO_SUMMARY, MARKDOWN_RICH)
            
        Returns:
            包含爬取结果的字典
        """
        for attempt in range(self.max_retry):
            request_id = f"goliath_retrieve_{int(time.time()*1000)}"
            
            # 使用简化的payload结构，与goliath_demo保持一致
            payload = {
                "biz_def": self.biz_def,
                "url": url,
                "timeout_ms": self.timeout_ms,
                "retrieve_type": retrieve_type,
                "model": self.model_name
            }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.rock_token}",
                "rock-request-id": request_id,
            }
            
            try:
                logger.info(f"正在爬取: {url}")
                response = requests.post(self.addr, data=json.dumps(payload), headers=headers, timeout=(self.timeout_ms/1000))
                return self._handle_response(response)
                    
            except Exception as e:
                logger.error(f"爬取尝试第 {attempt + 1} 次时出错: {e}")
                if attempt < self.max_retry - 1:
                    logger.info("等待5s后重试...")
                    time.sleep(5)
                continue
                
        return {"error": "所有重试尝试均失败"}

    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """处理响应"""
        try:
            response_data = response.json()
            
            # 检查是否有result字段
            if "result" in response_data:
                result = response_data["result"]
                
                # 解码base64内容
                content = result.get("content", "")
                if content:
                    try:
                        decoded_content = base64.b64decode(content).decode('utf-8')
                        result["content"] = decoded_content
                    except Exception as e:
                        logger.warning(f"解码content失败: {e}")

                return {
                    "success": True,
                    "request_id": response_data.get("request_id"),
                    "debug_string": response_data.get("debug_string", ""),
                    "result": result
                }
            else:
                return {
                    "success": False,
                    "error": "响应中未找到result字段",
                    "raw_response": response_data
                }

        except Exception as e:
            logger.error(f"解析响应时出错: {e}")
            return {
                "success": False,
                "error": f"解析响应失败: {e}",
                "raw_text": response.text
            }

    def __call__(self, url: str, retrieve_type="MARKDOWN_RICH", **kwargs) -> Dict[str, Any]:
        """
        调用接口
        
        Args:
            url: 要爬取的网址
            **kwargs: 其他参数

        Returns:
            爬取结果
        """
        try:
            response_dict = self.retrieve(url, retrieve_type, **kwargs)
            if response_dict.get("success"):
                return {
                    'success': True,
                    'url': response_dict.get("result", {}).get("url", ""),
                    'title': response_dict.get("result", {}).get("title", ""),
                    'description': response_dict.get("result", {}).get("decscription", ""),
                    'content': response_dict.get("result", {}).get("content", ""),
                    'publish_time': response_dict.get("result", {}).get("publish_time", "")
                }
            else:
                return {
                    'success': False,
                    'url': url,
                    'title': '',
                    'content': '',
                    "error": response_dict.get("error", "Unknown error")
                }
        except Exception as e:
            logger.error(f"爬取失败: {e}")
            return {
                'success': False,
                'url': url,
                'title': '',
                'content': '',
                'error': str(e)
            }


def build_default_goliath_tool() -> GoliathTool:
    """提供一个可复用的默认实例，供引用抓取脚本直接调用。"""
    return GoliathTool(
        addr=DEFAULT_GOLIATH_ADDR,
        rock_token=DEFAULT_GOLIATH_TOKEN,
        timeout_ms=DEFAULT_GOLIATH_TIMEOUT_MS,
        max_retry=DEFAULT_GOLIATH_MAX_RETRY,
        biz_def=DEFAULT_GOLIATH_BIZ_DEF,
    )


def test_batch_crawl():
    """测试批量爬取功能：从web_pages.jsonl采样1000条数据并爬取"""
    print("🚀 开始批量爬取测试...")
    
    # 文件路径
    source_file = "/mnt/jfs/xubenfeng/ys_agent_dev/thinkingagent/data/context_reward/data/web_pages.jsonl"
    sample_file = "/mnt/jfs/xubenfeng/ys_agent_dev/thinkingagent/data/context_reward/data/web_pages_sample1000.jsonl"
    output_dir = "/mnt/jfs/xubenfeng/ys_agent_dev/ys_agent_dev/tools/data"
    
    # 第一步：从原始文件采样1000条数据
    # print("📁 步骤1: 从原始文件采样1000条数据...")
    
    # 读取所有数据
    # all_data = []
    # with open(source_file, 'r', encoding='utf-8') as f:
    #     for line in f:
    #         line = line.strip()
    #         if line:
    #             try:
    #                 data = json.loads(line)
    #                 all_data.append(data)
    #             except json.JSONDecodeError:
    #                 continue
    
    # print(f"原始文件共有 {len(all_data)} 条数据")
    
    # # 随机采样100条
    # sample_size = min(1000, len(all_data))
    # sampled_data = random.sample(all_data, sample_size)
    
    # # 保存采样数据
    # with open(sample_file, 'w', encoding='utf-8') as f:
    #     for data in sampled_data:
    #         f.write(json.dumps(data, ensure_ascii=False) + '\n')
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 第一步：读取采样1000条数据
    
    # 读取所有数据
    sampled_data = []
    with open(sample_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    data = json.loads(line)
                    sampled_data.append(data)
                except json.JSONDecodeError:
                    continue
    
    sample_size = len(sampled_data)
    print(f"✅ 已采样 {sample_size} 条数据并保存到: {sample_file}")
    
    # 第二步：使用GoliathTool爬取这些网页
    print("\n🕷️  步骤2: 开始批量爬取网页...")
    
    # 创建工具实例
    tool = GoliathTool(
        addr="https://rockmgrgo.wenxiaobai.com/rockopenai/v1/web_search",
        rock_token="rock-ShWAczRU3K8Pk12p6NQalR3JP1GD7pUhemTwNnmD7PW26",
        timeout_ms=60000, 
        max_retry=3, 
        biz_def="batch_test"
    )
    
    # 爬取结果
    crawl_results = []
    success_count = 0
    fail_count = 0
    
    for i, data in enumerate(sampled_data):
        url = data.get("url", "")
        title = data.get("title", "")
        
        print(f"[{i+1}/{sample_size}] 爬取: {title[:50]}...")
        
        start_time = time.time()
        result = tool.retrieve(url=url, retrieve_type="MARKDOWN_RICH")
        end_time = time.time()
        
        crawl_time = end_time - start_time
        
        if result.get("success"):
            success_count += 1
            content = result.get("result", {}).get("content", "")
            crawl_results.append({
                "index": i + 1,
                "url": url,
                "original_title": title,
                "crawl_success": True,
                "crawl_time": round(crawl_time, 2),
                "content_length": len(content),
                "content": content,
                "request_id": result.get("request_id", ""),
                "debug_info": result.get("debug_string", "")
            })
            print(f"  ✅ 成功，耗时: {crawl_time:.2f}秒，内容: {len(content)}字符")
        else:
            fail_count += 1
            crawl_results.append({
                "index": i + 1,
                "url": url,
                "original_title": title,
                "crawl_success": False,
                "crawl_time": round(crawl_time, 2),
                "content_length": 0,
                "content": "",
                "error": result.get("error", "Unknown error")
            })
            print(f"  ❌ 失败，耗时: {crawl_time:.2f}秒，错误: {result.get('error', 'Unknown')}")
        
        # 添加短暂延迟避免过于频繁的请求
        if i < sample_size - 1:  # 最后一个请求不需要延迟
            time.sleep(1)
    
    # 第三步：保存爬取结果
    print(f"\n💾 步骤3: 保存爬取结果...")
    
    # 保存详细结果
    results_file = os.path.join(output_dir, f"goliath_crawl_results_{int(time.time())}.json")
    with open(results_file, 'w', encoding='utf-8') as f:
        json.dump(crawl_results, f, ensure_ascii=False, indent=2)
    
    # 统计报告
    total_time = sum(r["crawl_time"] for r in crawl_results)
    avg_time = total_time / len(crawl_results) if crawl_results else 0
    total_content = sum(r["content_length"] for r in crawl_results if r["crawl_success"])
    
    print(f"\n📊 批量爬取完成！统计报告:")
    print(f"  总计: {sample_size} 个网页")
    print(f"  成功: {success_count} 个")
    print(f"  失败: {fail_count} 个")
    print(f"  成功率: {success_count/sample_size*100:.1f}%")
    print(f"  总耗时: {total_time:.2f}秒")
    print(f"  平均耗时: {avg_time:.2f}秒/个")
    print(f"  总内容: {total_content} 字符")
    print(f"  结果文件: {results_file}")
    print("=" * 60)


if __name__ == "__main__":
    # 创建 Goliath 工具实例
    tool = build_default_goliath_tool()

    print("=== 测试网页爬取功能 ===")
    # 测试爬取功能
    result = tool(
        # url="https://www.axtonliu.com/ai-ethics-trolley-problem-experiment/",
        # url="https://www.zhihu.com/question/442922646/answer/1729264160"
        # url="https://www.zhihu.com/question/399526996/answer/1266743626"
        url="https://36kr.com/p/3429634800012934"
        )
    print(result)
    
    # 保存结果到文件
    if result.get("success"):
        output_dir = "/home/inter_wangpengyu/Wiki_Challenge/wiki_data/"
        os.makedirs(output_dir, exist_ok=True)
        
        title = result.get("title", "untitled").replace("/", "_").replace("\\", "_")
        content = result.get("content", "")
        
        filename = f"{title}.md"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ 内容已保存到: {filepath}")
    
    # if result.get("success"):
    #     print("爬取成功!")
    #     print(f"Request ID: {result.get('request_id')}")
    #     print(f"Debug: {result.get('debug_string')}")
    #     content = result.get("result", {}).get("content", "")
    #     print(f"内容长度: {len(content)} 字符")
    #     print("内容预览:")
    #     print(content[:500] + "..." if len(content) > 500 else content)
    # else:
    #     print("爬取失败:")
    #     print(json.dumps(result, ensure_ascii=False, indent=2))

    # 批量测试
    # print("\n" + "="*60)
    # test_batch_crawl()



