import re
import json
import jsonlines
import os
from local_models import oss_llm_complete

def batch_eval(query_file, result1_file, result2_file, output_file_path):
    """批量评估两个结果文件"""
    
    # 读取查询
    with open(query_file, "r", encoding="utf-8") as f:
        data = f.read()
    queries = re.findall(r"- Question \d+: (.+)", data)

    # 读取结果1
    with open(result1_file, "r", encoding="utf-8") as f:
        answers1 = json.load(f)
    answers1 = [i["result"] for i in answers1]

    # 读取结果2
    with open(result2_file, "r", encoding="utf-8") as f:
        answers2 = json.load(f)
    answers2 = [i["result"] for i in answers2]

    print(f"查询数量: {len(queries)}")
    print(f"结果1数量: {len(answers1)}")
    print(f"结果2数量: {len(answers2)}")

    if len(queries) != len(answers1) or len(queries) != len(answers2):
        print("❌ 查询和结果数量不匹配！")
        return

    evaluations = []
    
    for i, (query, answer1, answer2) in enumerate(zip(queries, answers1, answers2)):
        print(f"正在评估第 {i+1}/{len(queries)} 个查询...")
        
        sys_prompt = """
        ---Role---
        You are an expert tasked with evaluating two answers to the same question based on three criteria: **Comprehensiveness**, **Diversity**, and **Empowerment**.
        """

        prompt = f"""
        You will evaluate two answers to the same question based on three criteria: **Comprehensiveness**, **Diversity**, and **Empowerment**.

        - **Comprehensiveness**: How much detail does the answer provide to cover all aspects and details of the question?
        - **Diversity**: How varied and rich is the answer in providing different perspectives and insights on the question?
        - **Empowerment**: How well does the answer help the reader understand and make informed judgments about the topic?

        For each criterion, choose the better answer (either Answer 1 or Answer 2) and explain why. Then, select an overall winner based on these three categories.

        Here is the question:
        {query}

        Here are the two answers:

        **Answer 1:**
        {answer1}

        **Answer 2:**
        {answer2}

        Evaluate both answers using the three criteria listed above and provide detailed explanations for each criterion.

        Output your evaluation in the following JSON format:

        {{
            "Comprehensiveness": {{
                "Winner": "[Answer 1 or Answer 2]",
                "Explanation": "[Provide explanation here]"
            }},
            "Diversity": {{
                "Winner": "[Answer 1 or Answer 2]",
                "Explanation": "[Provide explanation here]"
            }},
            "Empowerment": {{
                "Winner": "[Answer 1 or Answer 2]",
                "Explanation": "[Provide explanation here]"
            }},
            "Overall Winner": {{
                "Winner": "[Answer 1 or Answer 2]",
                "Explanation": "[Summarize why this answer is the overall winner based on the three criteria]"
            }}
        }}
        """

        try:
            # 使用OSS API进行评估
            evaluation_result = oss_llm_complete(
                prompt=prompt,
                system_prompt=sys_prompt,
                max_tokens=2048,
                temperature=0.1  # 降低温度以获得更一致的评估
            )
            
            # 尝试解析JSON结果
            try:
                # 提取JSON部分
                json_start = evaluation_result.find('{')
                json_end = evaluation_result.rfind('}') + 1
                if json_start != -1 and json_end > json_start:
                    json_str = evaluation_result[json_start:json_end]
                    evaluation_data = json.loads(json_str)
                else:
                    evaluation_data = {"error": "无法找到有效的JSON格式", "raw_response": evaluation_result}
            except json.JSONDecodeError:
                evaluation_data = {"error": "JSON解析失败", "raw_response": evaluation_result}
            
            evaluation_entry = {
                "query_index": i,
                "query": query,
                "evaluation": evaluation_data
            }
            
            evaluations.append(evaluation_entry)
            print(f"✅ 第 {i+1} 个查询评估完成")
            
        except Exception as e:
            print(f"❌ 第 {i+1} 个查询评估失败: {e}")
            evaluation_entry = {
                "query_index": i,
                "query": query,
                "evaluation": {"error": str(e)}
            }
            evaluations.append(evaluation_entry)

    # 保存评估结果
    with open(output_file_path, "w", encoding="utf-8") as output_file:
        json.dump(evaluations, output_file, ensure_ascii=False, indent=2)
    
    print(f"✅ 评估完成，结果保存到: {output_file_path}")
    
    # 统计结果
    analyze_evaluation_results(evaluations)

def analyze_evaluation_results(evaluations):
    """分析评估结果"""
    print("\n" + "="*50)
    print("评估结果统计")
    print("="*50)
    
    total_valid = 0
    criteria_stats = {
        "Comprehensiveness": {"Answer 1": 0, "Answer 2": 0},
        "Diversity": {"Answer 1": 0, "Answer 2": 0},
        "Empowerment": {"Answer 1": 0, "Answer 2": 0},
        "Overall Winner": {"Answer 1": 0, "Answer 2": 0}
    }
    
    for eval_entry in evaluations:
        evaluation = eval_entry.get("evaluation", {})
        if "error" not in evaluation:
            total_valid += 1
            for criterion in criteria_stats:
                winner = evaluation.get(criterion, {}).get("Winner", "")
                if winner in ["Answer 1", "Answer 2"]:
                    criteria_stats[criterion][winner] += 1
    
    print(f"有效评估数量: {total_valid}/{len(evaluations)}")
    
    for criterion, stats in criteria_stats.items():
        total = stats["Answer 1"] + stats["Answer 2"]
        if total > 0:
            pct1 = (stats["Answer 1"] / total) * 100
            pct2 = (stats["Answer 2"] / total) * 100
            print(f"\n{criterion}:")
            print(f"  Answer 1: {stats['Answer 1']} ({pct1:.1f}%)")
            print(f"  Answer 2: {stats['Answer 2']} ({pct2:.1f}%)")

def main():
    """主函数"""
    # 从环境变量读取类别，默认为agriculture
    cls = os.getenv("CLASS", "agriculture")
    mode1 = "naive"      # 第一个要比较的模式
    mode2 = "hybrid"     # 第二个要比较的模式
    
    print(f"📊 评估数据集类别: {cls}")
    print(f"📈 比较模式: {mode1} vs {mode2}")
    
    # 文件路径
    query_file = f"../exp_results/data/questions/{cls}_questions.txt"
    result1_file = f"../exp_results/data/results/{cls}_{mode1}_results.json"
    result2_file = f"../exp_results/data/results/{cls}_{mode2}_results.json"
    
    # 检查文件是否存在
    files_to_check = [query_file, result1_file, result2_file]
    for file_path in files_to_check:
        if not os.path.exists(file_path):
            print(f"❌ 文件不存在: {file_path}")
            print("请确保已运行完整的实验流程")
            return
    
    # 创建评估结果目录
    eval_dir = "../exp_results/data/evaluations"
    os.makedirs(eval_dir, exist_ok=True)
    
    # 输出文件
    output_file = f"{eval_dir}/{cls}_{mode1}_vs_{mode2}_evaluation.json"
    
    print(f"开始评估: {mode1} vs {mode2}")
    print(f"类别: {cls}")
    print(f"查询文件: {query_file}")
    print(f"结果1文件: {result1_file}")
    print(f"结果2文件: {result2_file}")
    print(f"输出文件: {output_file}")
    
    batch_eval(query_file, result1_file, result2_file, output_file)

if __name__ == "__main__":
    main()
