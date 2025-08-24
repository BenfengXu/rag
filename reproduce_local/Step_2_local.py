import json
import os
from transformers import AutoTokenizer
from local_models import oss_llm_complete

# 加载OSS模型的tokenizer
OSS_MODEL_PATH = "/mnt/jfs/xubenfeng/model/gpt-oss-120b"
oss_tokenizer = AutoTokenizer.from_pretrained(OSS_MODEL_PATH)

def get_summary(context, tot_tokens=2000):
    """提取文档摘要（使用OSS tokenizer，完全按照原始逻辑）"""
    # 完全按照原始Step_2.py的逻辑
    tokens = oss_tokenizer.tokenize(context)
    half_tokens = tot_tokens // 2

    start_tokens = tokens[1000 : 1000 + half_tokens]
    end_tokens = tokens[-(1000 + half_tokens) : -1000] if len(tokens) > 1000 else tokens[-(1000 + half_tokens):]

    summary_tokens = start_tokens + end_tokens
    summary = oss_tokenizer.convert_tokens_to_string(summary_tokens)

    return summary

def generate_questions_for_class(cls):
    """为指定类别生成问题"""
    unique_contexts_file = f"../exp_results/data/unique_contexts/{cls}_unique_contexts.json"
    
    if not os.path.exists(unique_contexts_file):
        print(f"❌ 文件不存在: {unique_contexts_file}")
        return
    
    print(f"正在处理类别: {cls}")
    print(f"读取文件: {unique_contexts_file}")
    
    with open(unique_contexts_file, mode="r") as f:
        unique_contexts = json.load(f)

    print(f"文档数量: {len(unique_contexts)}")
    
    # 生成摘要
    print("正在生成文档摘要...")
    summaries = [get_summary(context) for context in unique_contexts]
    total_description = "\n\n".join(summaries)

    prompt = f"""
Given the following description of a dataset:

<description>
{total_description}
</description>

<instructions>
Please identify 5 potential users who would engage with this dataset. For each user, list 5 tasks they would perform with this dataset. Then, for each (user, task) combination, generate 5 questions that require a high-level understanding of the entire dataset.
</instructions>

Output the results in the following structure (with exact format):
```
- User 1: [user description]
    - Task 1: [task description]
        - Question 1:
        - Question 2:
        - Question 3:
        - Question 4:
        - Question 5:
    - Task 2: [task description]
        ...
    - Task 5: [task description]
- User 2: [user description]
    ...
- User 5: [user description]
    ...
```
"""

    print("正在调用OSS API生成问题...")
    result = oss_llm_complete(prompt=prompt, max_tokens=4096, temperature=0.7)

    # 创建输出目录
    questions_dir = "../exp_results/data/questions"
    os.makedirs(questions_dir, exist_ok=True)
    
    # 保存结果
    file_path = f"{questions_dir}/{cls}_questions.txt"
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(result)

    print(f"✅ {cls}_questions 已保存到 {file_path}")
    return result

def main():
    """主函数"""
    # 从环境变量读取类别，默认为agriculture
    cls = os.getenv("CLASS", "agriculture")
    classes_to_process = [cls]
    print(f"📊 处理数据集类别: {cls}")
    
    for cls in classes_to_process:
        try:
            generate_questions_for_class(cls)
            print(f"✅ {cls} 处理完成\n")
        except Exception as e:
            print(f"❌ 处理 {cls} 时出错: {e}\n")

if __name__ == "__main__":
    main()
