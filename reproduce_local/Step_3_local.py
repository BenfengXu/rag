import re
import json
import os
import asyncio
from lightrag import LightRAG, QueryParam
from lightrag.utils import EmbeddingFunc
from lightrag.kg.shared_storage import initialize_pipeline_status
from local_models import (
    lightrag_llm_func_async, 
    lightrag_embedding_func_async, 
    get_embedding_dim,
    init_qwen_embedding
)

def extract_queries(file_path):
    """从生成的问题文件中提取查询"""
    with open(file_path, "r", encoding="utf-8") as f:
        data = f.read()

    # 完全按照原始Step_3.py的逻辑
    data = data.replace("**", "")
    queries = re.findall(r"- Question \d+: (.+)", data)
    
    return queries

async def process_query(query_text, rag_instance, query_param):
    """处理单个查询"""
    try:
        result = await rag_instance.aquery(query_text, param=query_param)
        return {"query": query_text, "result": result}, None
    except Exception as e:
        return None, {"query": query_text, "error": str(e)}

async def run_queries_and_save_to_json(
    queries, rag_instance, query_param, output_file, error_file
):
    """运行查询并保存结果"""
    with (
        open(output_file, "w", encoding="utf-8") as result_file,
        open(error_file, "w", encoding="utf-8") as err_file,
    ):
        result_file.write("[\n")
        first_entry = True

        for i, query_text in enumerate(queries):
            print(f"处理查询 {i+1}/{len(queries)}: {query_text[:100]}...")
            
            result, error = await process_query(query_text, rag_instance, query_param)

            if result:
                if not first_entry:
                    result_file.write(",\n")
                json.dump(result, result_file, ensure_ascii=False, indent=4)
                first_entry = False
                print(f"✅ 查询 {i+1} 完成")
            elif error:
                json.dump(error, err_file, ensure_ascii=False, indent=4)
                err_file.write("\n")
                print(f"❌ 查询 {i+1} 失败: {error['error']}")

        result_file.write("\n]")

async def initialize_rag(cls):
    """初始化RAG实例"""
    working_dir = f"../{cls}"
    
    if not os.path.exists(working_dir):
        print(f"❌ 工作目录不存在: {working_dir}")
        print("请先运行 Step_1_local.py 来构建知识图谱")
        return None
    
    print("正在初始化本地模型...")
    
    # 预先初始化Qwen Embedding模型
    init_qwen_embedding()
    embedding_dim = get_embedding_dim()
    print(f"✅ Embedding维度: {embedding_dim}")
    
    # 创建LightRAG实例
    rag = LightRAG(
        working_dir=working_dir,
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

async def process_class_queries(cls, query_modes=["naive", "local", "global", "hybrid"]):
    """处理指定类别的查询"""
    print(f"\n开始处理类别: {cls}")
    
    # 检查问题文件是否存在
    questions_file = f"../exp_results/data/questions/{cls}_questions.txt"
    if not os.path.exists(questions_file):
        print(f"❌ 问题文件不存在: {questions_file}")
        print("请先运行 Step_2_local.py 来生成问题")
        return
    
    # 提取查询
    queries = extract_queries(questions_file)
    print(f"提取到 {len(queries)} 个查询")
    
    if len(queries) == 0:
        print("❌ 没有找到有效的查询")
        return
    
    # 初始化RAG实例
    rag = await initialize_rag(cls)
    if rag is None:
        return
    
    # 创建结果目录
    results_dir = "../exp_results/data/results"
    os.makedirs(results_dir, exist_ok=True)
    
    # 对每种查询模式运行查询
    for mode in query_modes:
        print(f"\n正在运行 {mode} 模式查询...")
        
        query_param = QueryParam(mode=mode, enable_rerank=False)
        output_file = f"{results_dir}/{cls}_{mode}_results.json"
        error_file = f"{results_dir}/{cls}_{mode}_errors.json"
        
        await run_queries_and_save_to_json(
            queries, rag, query_param, output_file, error_file
        )
        
        print(f"✅ {mode} 模式查询完成，结果保存到: {output_file}")

async def main():
    """主函数"""
    # 从环境变量读取类别，默认为agriculture
    cls = os.getenv("CLASS", "agriculture")
    classes_to_process = [cls]
    query_modes = ["naive", "local", "global", "hybrid"]  # 可以选择性移除某些模式
    
    print(f"📊 处理数据集类别: {cls}")
    print(f"🔍 查询模式: {', '.join(query_modes)}")
    
    for cls in classes_to_process:
        try:
            await process_class_queries(cls, query_modes)
            print(f"\n✅ {cls} 所有查询模式处理完成")
        except Exception as e:
            print(f"\n❌ 处理 {cls} 时出错: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
