import re
import json
import os
from collections import defaultdict

def analyze_question_granularity(questions_file, contexts_dir):
    """分析问题的粒度和相关性"""
    
    # 读取生成的问题
    with open(questions_file, 'r', encoding='utf-8') as f:
        questions_text = f.read()
    
    questions = re.findall(r"- Question \d+: (.+)", questions_text)
    
    # 分析问题特征
    analysis = {
        "total_questions": len(questions),
        "macro_questions": 0,  # 宏观问题数量
        "micro_questions": 0,  # 细粒度问题数量
        "keyword_distribution": defaultdict(int),
        "question_types": {
            "comparison": 0,
            "summary": 0, 
            "specific_fact": 0,
            "integration": 0
        }
    }
    
    # 宏观问题关键词
    macro_keywords = [
        "overall", "general", "main", "primary", "key", "important",
        "summary", "overview", "compare", "difference", "similarity",
        "framework", "approach", "methodology", "strategy"
    ]
    
    # 细粒度问题关键词
    micro_keywords = [
        "specific", "detail", "exactly", "precisely", "particular",
        "step", "parameter", "value", "implementation", "code",
        "algorithm", "formula", "equation", "example"
    ]
    
    for question in questions:
        question_lower = question.lower()
        
        # 统计关键词
        macro_count = sum(1 for kw in macro_keywords if kw in question_lower)
        micro_count = sum(1 for kw in micro_keywords if kw in question_lower)
        
        if macro_count > micro_count:
            analysis["macro_questions"] += 1
        else:
            analysis["micro_questions"] += 1
            
        # 问题类型分类
        if any(word in question_lower for word in ["compare", "difference", "versus"]):
            analysis["question_types"]["comparison"] += 1
        elif any(word in question_lower for word in ["summary", "overview", "main"]):
            analysis["question_types"]["summary"] += 1
        elif any(word in question_lower for word in ["how", "what", "which", "specific"]):
            analysis["question_types"]["specific_fact"] += 1
        elif any(word in question_lower for word in ["integrate", "combine", "relationship"]):
            analysis["question_types"]["integration"] += 1
    
    return analysis, questions

def generate_fine_grained_questions(contexts_dir, output_file):
    """生成细粒度问题，基于具体事实"""
    
    fine_grained_questions = []
    
    # 遍历所有context文件
    for filename in os.listdir(contexts_dir):
        if filename.endswith('.txt'):
            filepath = os.path.join(contexts_dir, filename)
            
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 提取具体数字、名称、参数等
            numbers = re.findall(r'\d+\.?\d*', content)
            algorithms = re.findall(r'[A-Z][a-z]+(?:[A-Z][a-z]+)*(?=\s+algorithm|Algorithm)', content)
            parameters = re.findall(r'(\w+)\s*=\s*(\d+\.?\d*)', content)
            
            # 基于具体内容生成问题
            if numbers:
                fine_grained_questions.append(f"What is the exact value of {numbers[0]} mentioned in {filename}?")
            
            if algorithms:
                fine_grained_questions.append(f"What are the specific steps of the {algorithms[0]} algorithm described in {filename}?")
                
            if parameters:
                param_name, param_value = parameters[0]
                fine_grained_questions.append(f"Why is {param_name} set to {param_value} in {filename}?")
    
    # 保存细粒度问题
    with open(output_file, 'w', encoding='utf-8') as f:
        for i, question in enumerate(fine_grained_questions, 1):
            f.write(f"- Question {i}: {question}\n")
    
    return fine_grained_questions

if __name__ == "__main__":
    # 分析现有问题质量
    questions_file = "../exp_results/data/questions/cs_questions.txt"
    contexts_dir = "../exp_results/data/unique_contexts"
    
    if os.path.exists(questions_file):
        analysis, questions = analyze_question_granularity(questions_file, contexts_dir)
        
        print("📊 问题质量分析报告")
        print("=" * 50)
        print(f"总问题数: {analysis['total_questions']}")
        print(f"宏观问题: {analysis['macro_questions']} ({analysis['macro_questions']/analysis['total_questions']*100:.1f}%)")
        print(f"细粒度问题: {analysis['micro_questions']} ({analysis['micro_questions']/analysis['total_questions']*100:.1f}%)")
        print("\n问题类型分布:")
        for qtype, count in analysis['question_types'].items():
            print(f"  {qtype}: {count}")
        
        # 生成改进的细粒度问题
        fine_grained_output = "../exp_results/data/questions/cs_fine_grained_questions.txt"
        fine_questions = generate_fine_grained_questions(contexts_dir, fine_grained_output)
        print(f"\n✅ 生成了 {len(fine_questions)} 个细粒度问题")
        print(f"保存到: {fine_grained_output}")