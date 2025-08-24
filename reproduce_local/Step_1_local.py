import os
import json
import time
import asyncio

from lightrag import LightRAG
from lightrag.utils import EmbeddingFunc
from lightrag.kg.shared_storage import initialize_pipeline_status
from local_models import (
    lightrag_llm_func_async, 
    lightrag_embedding_func_async, 
    get_embedding_dim,
    init_qwen_embedding,
    show_oss_config
)

async def insert_text(rag, file_path):
    """插入文本到LightRAG"""
    with open(file_path, mode="r") as f:
        unique_contexts = json.load(f)

    retries = 0
    max_retries = 3
    while retries < max_retries:
        try:
            # 使用异步插入
            await rag.ainsert(unique_contexts)
            print("✅ 文档插入成功!")
            break
        except Exception as e:
            retries += 1
            print(f"❌ 插入失败，重试 ({retries}/{max_retries}), 错误: {e}")
            await asyncio.sleep(10)  # 使用异步sleep
    if retries == max_retries:
        print("❌ 超过最大重试次数后插入失败")

# 配置参数
cls = os.getenv("CLASS", "agriculture")  # 从环境变量读取，默认为agriculture
WORKING_DIR = f"../exp_results/kg/{cls}"
print(f"📊 处理数据集类别: {cls}")
print(f"📁 工作目录: {WORKING_DIR}")

if not os.path.exists(WORKING_DIR):
    os.makedirs(WORKING_DIR, exist_ok=True)

async def initialize_rag():
    """初始化LightRAG实例"""
    print("正在初始化本地模型...")
    
    # 显示OSS负载均衡配置
    show_oss_config()
    
    # 预先初始化Qwen Embedding模型
    init_qwen_embedding()
    embedding_dim = get_embedding_dim()
    print(f"✅ Embedding维度: {embedding_dim}")
    
    # 创建LightRAG实例
    rag = LightRAG(
        working_dir=WORKING_DIR,
        llm_model_func=lightrag_llm_func_async,  # 使用OSS LLM
        embedding_func=EmbeddingFunc(
            embedding_dim=embedding_dim,
            func=lightrag_embedding_func_async  # 使用Qwen Embedding
        ),
    )

    await rag.initialize_storages()
    await initialize_pipeline_status()
    
    print("✅ LightRAG初始化完成!")
    return rag

async def main():
    """主函数"""
    print(f"开始处理数据集: {cls}")
    print(f"工作目录: {WORKING_DIR}")
    
    # 初始化RAG实例
    rag = await initialize_rag()
    
    # 插入文本
    data_file = f"../exp_results/data/unique_contexts/{cls}_unique_contexts.json"
    if os.path.exists(data_file):
        print(f"正在插入数据: {data_file}")
        await insert_text(rag, data_file)
    else:
        print(f"❌ 数据文件不存在: {data_file}")
        print("请先运行 Step_0.py 来处理数据集")

if __name__ == "__main__":
    asyncio.run(main())
