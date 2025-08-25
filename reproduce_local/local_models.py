#!/usr/bin/env python3
"""
本地模型配置文件
OSS API + Qwen3-Embedding-0.6B
"""

import requests
import json
import torch
import numpy as np
import os
from transformers import AutoTokenizer, AutoModel
from typing import List, Any, Dict
import asyncio
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ==================== OSS LLM 配置 ====================
import threading
import random

# OSS LLM负载均衡配置
_oss_counter = 0
_oss_lock = threading.Lock()

# ==================== Embedding服务配置 ====================
# Embedding服务负载均衡配置
_embedding_counter = 0
_embedding_lock = threading.Lock()
_use_remote_embedding = False  # 是否使用远程Embedding服务

def get_oss_config():
    """获取OSS配置，支持多端口负载均衡"""
    global _oss_counter
    
    # 从环境变量读取配置
    oss_host = os.getenv("OSS_HOST", "10.0.4.178")
    oss_ports = os.getenv("OSS_PORTS", "30066")
    
    # 解析端口列表
    if "," in oss_ports:
        ports_list = [port.strip() for port in oss_ports.split(",")]
    else:
        ports_list = [oss_ports]
    
    # 轮询选择端口
    with _oss_lock:
        port = ports_list[_oss_counter % len(ports_list)]
        _oss_counter += 1
    
    return {
        "url": f"http://{oss_host}:{port}/v1/chat/completions",
        "headers": {
            "Content-Type": "application/json",
            "Authorization": "Bearer dummy_token"
        },
        "model": "default",
        "port": port,
        "host": oss_host
    }

def get_embedding_config():
    """获取Embedding配置，支持多端口负载均衡"""
    global _embedding_counter
    
    # 从环境变量读取配置
    embedding_host = os.getenv("EMBEDDING_HOST", "10.0.4.178")
    embedding_ports = os.getenv("EMBEDDING_PORTS", "30151")
    
    # 解析端口列表
    if "," in embedding_ports:
        ports_list = [port.strip() for port in embedding_ports.split(",")]
    else:
        ports_list = [embedding_ports]
    
    # 轮询选择端口
    with _embedding_lock:
        port = ports_list[_embedding_counter % len(ports_list)]
        _embedding_counter += 1
    
    return {
        "url": f"http://{embedding_host}:{port}/v1/embeddings",
        "headers": {
            "Content-Type": "application/json"
        },
        "port": port,
        "host": embedding_host
    }

def enable_remote_embedding():
    """启用远程Embedding服务"""
    global _use_remote_embedding
    _use_remote_embedding = True
    print("✅ 已启用远程Embedding服务")

def disable_remote_embedding():
    """禁用远程Embedding服务，使用本地模型"""
    global _use_remote_embedding
    _use_remote_embedding = False
    print("✅ 已切换到本地Embedding模型")

# ==================== Qwen Embedding 配置 ====================
QWEN_MODEL_PATH = "/mnt/jfs/xubenfeng/rag/models_and_datasets/Qwen3-Embedding-0.6B"

# 全局变量存储模型
_tokenizer = None
_model = None
_device = None

def filter_json_serializable_kwargs(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """过滤出可以JSON序列化的参数"""
    filtered_kwargs = {}
    
    # 只保留这些类型的参数，这些是OpenAI API可能需要的
    allowed_params = {
        'temperature', 'max_tokens', 'top_p', 'frequency_penalty', 
        'presence_penalty', 'stop', 'stream', 'logit_bias', 'user',
        'seed', 'top_logprobs', 'logprobs'
    }
    
    for key, value in kwargs.items():
        # 只处理允许的参数名
        if key not in allowed_params:
            # print(f"🔧 跳过非API参数: {key} = {type(value)}")
            continue
            
        # 检查值的类型
        if isinstance(value, (str, int, float, bool, type(None))):
            filtered_kwargs[key] = value
        elif isinstance(value, (list, dict)):
            try:
                # 尝试序列化复杂类型
                json.dumps(value)
                filtered_kwargs[key] = value
            except (TypeError, ValueError):
                print(f"🔧 跳过不能序列化的参数: {key} = {type(value)}")
        else:
            print(f"🔧 跳过不支持的类型: {key} = {type(value)}")
    
    return filtered_kwargs

def init_qwen_embedding():
    """初始化Qwen Embedding模型"""
    global _tokenizer, _model, _device
    
    if _tokenizer is None:
        print("正在加载Qwen3-Embedding-0.6B模型...")
        _tokenizer = AutoTokenizer.from_pretrained(QWEN_MODEL_PATH, trust_remote_code=True)
        _model = AutoModel.from_pretrained(QWEN_MODEL_PATH, trust_remote_code=True)
        _model.eval()
        
        _device = "cuda" if torch.cuda.is_available() else "cpu"
        _model = _model.to(_device)
        print(f"✅ Qwen Embedding模型加载完成! 设备: {_device}")

def oss_llm_complete(
    prompt, 
    system_prompt=None, 
    history_messages=[], 
    model="default",
    max_tokens=8192,
    temperature=0.7,
    **kwargs
) -> str:
    """
    OSS LLM同步调用函数，支持负载均衡
    """
    # 获取负载均衡的配置
    oss_config = get_oss_config()
    
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    
    # 添加历史消息
    messages.extend(history_messages)
    messages.append({"role": "user", "content": prompt})
    
    # 过滤可序列化的kwargs
    filtered_kwargs = filter_json_serializable_kwargs(kwargs)
    
    data = {
        "model": oss_config["model"],
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
        **filtered_kwargs
    }
    
    try:
        response = requests.post(
            oss_config["url"], 
            headers=oss_config["headers"], 
            json=data, 
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            
            # 处理thinking模式的输出
            if "analysis" in content and "assistantfinal" in content:
                assistantfinal_start = content.find("assistantfinal")
                if assistantfinal_start != -1:
                    content = content[assistantfinal_start:].replace("assistantfinal", "").strip()
            
            # 输出负载均衡信息（可选，调试时使用）
            # print(f"🔄 使用OSS服务: {oss_config['host']}:{oss_config['port']}")
            
            return content
        else:
            print(f"❌ OSS API请求失败! 服务: {oss_config['host']}:{oss_config['port']}, 状态码: {response.status_code}")
            print(f"错误信息: {response.text}")
            return f"Error: {response.status_code} - {response.text}"
            
    except Exception as e:
        print(f"❌ OSS API调用异常: 服务: {oss_config['host']}:{oss_config['port']}, 错误: {e}")
        return f"Error: {str(e)}"

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((
        asyncio.TimeoutError,
        aiohttp.ClientError,
        ConnectionError,
        OSError
    ))
)
async def oss_llm_complete_async(
    prompt, 
    system_prompt=None, 
    history_messages=[], 
    model="default",
    max_tokens=8192,
    temperature=0.7,
    **kwargs
) -> str:
    """
    OSS LLM异步调用函数，支持负载均衡
    """
    # 获取负载均衡的配置
    oss_config = get_oss_config()
    
    messages = []
    if system_prompt:
        messages.append({"role": "system", "content": system_prompt})
    
    messages.extend(history_messages)
    messages.append({"role": "user", "content": prompt})
    
    # 过滤可序列化的kwargs
    filtered_kwargs = filter_json_serializable_kwargs(kwargs)
    
    data = {
        "model": oss_config["model"],
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
        **filtered_kwargs
    }
    
    try:
        # 创建连接器，限制并发连接数
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(
                oss_config["url"], 
                headers=oss_config["headers"], 
                json=data,
                timeout=aiohttp.ClientTimeout(total=600)  # 增加超时时间
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    content = result["choices"][0]["message"]["content"]
                    
                    # 处理thinking模式的输出
                    if "analysis" in content and "assistantfinal" in content:
                        assistantfinal_start = content.find("assistantfinal")
                        if assistantfinal_start != -1:
                            content = content[assistantfinal_start:].replace("assistantfinal", "").strip()
                    
                    # 输出负载均衡信息（可选，调试时使用）
                    # print(f"🔄 使用OSS服务: {oss_config['host']}:{oss_config['port']}")
                    
                    return content
                else:
                    error_text = await response.text()
                    print(f"❌ OSS API请求失败! 服务: {oss_config['host']}:{oss_config['port']}, 状态码: {response.status}")
                    print(f"错误信息: {error_text}")
                    return f"Error: {response.status} - {error_text}"
                    
    except asyncio.TimeoutError:
        print(f"⏰ OSS API超时: 服务: {oss_config['host']}:{oss_config['port']} (120秒)")
        return "Error: Timeout after 120 seconds"
    except aiohttp.ClientError as e:
        print(f"🌐 OSS API连接错误: 服务: {oss_config['host']}:{oss_config['port']}, 错误: {e}")
        return f"Error: Connection error - {str(e)}"
    except Exception as e:
        print(f"❌ OSS API异步调用异常: 服务: {oss_config['host']}:{oss_config['port']}, 错误类型: {type(e).__name__}, 详情: {e}")
        return f"Error: {type(e).__name__} - {str(e)}"

# ==================== 远程Embedding调用函数 ====================

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type((
        asyncio.TimeoutError,
        aiohttp.ClientError,
        ConnectionError,
        OSError
    ))
)
async def remote_embedding_async(texts: List[str]) -> np.ndarray:
    """远程Embedding异步调用函数，支持负载均衡"""
    embedding_config = get_embedding_config()
    
    data = {
        "input": texts,
        "model": "qwen-embedding"
    }
    
    try:
        # 创建连接器，限制并发连接数
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=10)
        async with aiohttp.ClientSession(connector=connector) as session:
            async with session.post(
                embedding_config["url"],
                headers=embedding_config["headers"],
                json=data,
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                
                if response.status == 200:
                    result = await response.json()
                    embeddings = []
                    for item in result["data"]:
                        embeddings.append(item["embedding"])
                    return np.array(embeddings, dtype=np.float32)
                else:
                    error_text = await response.text()
                    print(f"❌ Embedding API请求失败! 服务: {embedding_config['host']}:{embedding_config['port']}, 状态码: {response.status}")
                    print(f"错误信息: {error_text}")
                    raise RuntimeError(f"Embedding API Error: {response.status} - {error_text}")
                    
    except asyncio.TimeoutError:
        print(f"⏰ Embedding API超时: 服务: {embedding_config['host']}:{embedding_config['port']} (60秒)")
        raise
    except aiohttp.ClientError as e:
        print(f"🌐 Embedding API连接错误: 服务: {embedding_config['host']}:{embedding_config['port']}, 错误: {e}")
        raise
    except Exception as e:
        print(f"❌ Embedding API异步调用异常: 服务: {embedding_config['host']}:{embedding_config['port']}, 错误类型: {type(e).__name__}, 详情: {e}")
        raise

def remote_embedding(texts: List[str]) -> np.ndarray:
    """远程Embedding同步调用函数"""
    embedding_config = get_embedding_config()
    
    data = {
        "input": texts,
        "model": "qwen-embedding"
    }
    
    try:
        response = requests.post(
            embedding_config["url"],
            headers=embedding_config["headers"],
            json=data,
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            embeddings = []
            for item in result["data"]:
                embeddings.append(item["embedding"])
            return np.array(embeddings, dtype=np.float32)
        else:
            print(f"❌ Embedding API请求失败! 服务: {embedding_config['host']}:{embedding_config['port']}, 状态码: {response.status_code}")
            print(f"错误信息: {response.text}")
            raise RuntimeError(f"Embedding API Error: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"❌ Embedding API调用异常: 服务: {embedding_config['host']}:{embedding_config['port']}, 错误: {e}")
        raise e

def qwen_embedding(texts: List[str]) -> np.ndarray:
    """
    Qwen Embedding同步调用函数 - 支持远程/本地切换
    """
    global _use_remote_embedding, _tokenizer, _model, _device
    
    # 如果启用了远程服务，使用远程调用
    if _use_remote_embedding:
        return remote_embedding(texts)
    
    # 本地模式：确保模型已加载
    if _tokenizer is None:
        init_qwen_embedding()
    
    try:
        # Tokenize输入文本
        inputs = _tokenizer(
            texts, 
            padding=True, 
            truncation=True, 
            return_tensors="pt", 
            max_length=512
        )
        inputs = {k: v.to(_device) for k, v in inputs.items()}
        
        # 生成embeddings
        with torch.no_grad():
            outputs = _model(**inputs)
            # 使用mean pooling
            embeddings = outputs.last_hidden_state.mean(dim=1)
            embeddings = embeddings.cpu().numpy()
        
        return embeddings
        
    except Exception as e:
        print(f"❌ Qwen Embedding生成失败: {e}")
        raise e

async def qwen_embedding_async(texts: List[str]) -> np.ndarray:
    """
    Qwen Embedding异步调用函数 - 支持远程/本地切换
    """
    global _use_remote_embedding
    
    # 如果启用了远程服务，使用远程异步调用
    if _use_remote_embedding:
        return await remote_embedding_async(texts)
    
    # 本地模式：异步执行本地embedding
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, qwen_embedding, texts)

# ==================== LightRAG 兼容函数 ====================

def lightrag_llm_func(
    prompt, 
    system_prompt=None, 
    history_messages=[], 
    keyword_extraction=False,
    **kwargs
) -> str:
    """
    LightRAG兼容的LLM函数
    """
    return oss_llm_complete(
        prompt=prompt,
        system_prompt=system_prompt,
        history_messages=history_messages,
        **kwargs
    )

async def lightrag_llm_func_async(
    prompt, 
    system_prompt=None, 
    history_messages=[], 
    keyword_extraction=False,
    **kwargs
) -> str:
    """
    LightRAG兼容的异步LLM函数
    """
    return await oss_llm_complete_async(
        prompt=prompt,
        system_prompt=system_prompt,
        history_messages=history_messages,
        **kwargs
    )

def lightrag_embedding_func(texts: List[str]) -> np.ndarray:
    """
    LightRAG兼容的Embedding函数
    """
    return qwen_embedding(texts)

async def lightrag_embedding_func_async(texts: List[str]) -> np.ndarray:
    """
    LightRAG兼容的异步Embedding函数
    """
    return await qwen_embedding_async(texts)

# ==================== 获取Embedding维度 ====================
def get_embedding_dim() -> int:
    """获取Embedding维度"""
    # 用一个测试文本来获取维度
    test_embedding = qwen_embedding(["test"])
    return test_embedding.shape[1]

def show_oss_config():
    """显示当前OSS负载均衡配置"""
    oss_host = os.getenv("OSS_HOST", "10.0.4.178")
    oss_ports = os.getenv("OSS_PORTS", "30066")
    
    if "," in oss_ports:
        ports_list = [port.strip() for port in oss_ports.split(",")]
        print(f"🌐 OSS负载均衡配置:")
        print(f"  主机: {oss_host}")
        print(f"  端口数量: {len(ports_list)} 个")
        print(f"  端口列表: {', '.join(ports_list)}")
        print(f"  负载均衡: 轮询（Round Robin）")
    else:
        print(f"🌐 OSS单一服务配置:")
        print(f"  服务地址: {oss_host}:{oss_ports}")

def show_embedding_config():
    """显示当前Embedding负载均衡配置"""
    global _use_remote_embedding
    
    if _use_remote_embedding:
        embedding_host = os.getenv("EMBEDDING_HOST", "10.0.4.178")
        embedding_ports = os.getenv("EMBEDDING_PORTS", "30151")
        
        if "," in embedding_ports:
            ports_list = [port.strip() for port in embedding_ports.split(",")]
            print(f"🔮 Embedding远程服务配置:")
            print(f"  主机: {embedding_host}")
            print(f"  端口数量: {len(ports_list)} 个")
            print(f"  端口列表: {', '.join(ports_list)}")
            print(f"  负载均衡: 轮询（Round Robin）")
        else:
            print(f"🔮 Embedding单一远程服务配置:")
            print(f"  服务地址: {embedding_host}:{embedding_ports}")
    else:
        print(f"🔮 Embedding本地模型配置:")
        print(f"  模型路径: {QWEN_MODEL_PATH}")
        print(f"  运行模式: 本地GPU")

if __name__ == "__main__":
    # 显示配置信息
    show_oss_config()
    
    # 测试代码
    print("\n测试OSS LLM...")
    result = oss_llm_complete("你好，请介绍一下你自己。")
    print(f"OSS回复: {result[:100]}...")
    
    print("\n测试Qwen Embedding...")
    embeddings = qwen_embedding(["这是一个测试", "另一个测试"])
    print(f"Embedding形状: {embeddings.shape}")
    print(f"Embedding维度: {get_embedding_dim()}")
