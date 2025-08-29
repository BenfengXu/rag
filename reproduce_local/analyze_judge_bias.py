import asyncio
import json
import random
import os
import re
from local_models import lightrag_llm_func_async

class JudgeBiasAnalyzer:
    def __init__(self):
        self.results = {
            "position_bias": [],
            "length_bias": [],
            "trial_bias": [],  # 试次偏置
            "hybrid_vs_naive_comparison": []
        }
        self.questions = []
        self.hybrid_answers = {}
        self.naive_answers = {}
    
    def load_real_data(self, results_dir, questions_file):
        """加载真实的问题和答案数据"""
        
        # 加载问题
        try:
            with open(questions_file, 'r', encoding='utf-8') as f:
                questions_text = f.read()
            
            # 提取问题
            self.questions = re.findall(r"- Question \d+: (.+)", questions_text)
            print(f"✅ 加载了 {len(self.questions)} 个问题")
            
        except FileNotFoundError:
            print(f"❌ 问题文件不存在: {questions_file}")
            return False
        
        # 只加载hybrid和naive模式的答案
        modes = ["naive", "hybrid"]
        
        for mode in modes:
            result_file = os.path.join(results_dir, f"cs_{mode}_results.json")
            try:
                with open(result_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if mode == "hybrid":
                    for item in data:
                        query = item.get("query", "")
                        result = item.get("result", "")
                        self.hybrid_answers[query] = result
                elif mode == "naive":
                    for item in data:
                        query = item.get("query", "")
                        result = item.get("result", "")
                        self.naive_answers[query] = result
                
                print(f"✅ 加载了 {mode} 模式的 {len(data)} 个答案")
                
            except (FileNotFoundError, json.JSONDecodeError) as e:
                print(f"❌ 加载 {mode} 模式结果失败: {e}")
        
        return len(self.questions) > 0 and len(self.hybrid_answers) > 0 and len(self.naive_answers) > 0
    
    def find_all_hybrid_naive_pairs(self):
        """找到所有Hybrid和Naive的答案对"""
        
        answer_pairs = []
        
        # 遍历所有问题，寻找在两个模式中都有答案的问题
        for question in self.questions:
            hybrid_answer = None
            naive_answer = None
            
            # 在hybrid答案中查找匹配的问题
            for query, answer in self.hybrid_answers.items():
                if question in query or self._questions_match(question, query):
                    hybrid_answer = answer
                    break
            
            # 在naive答案中查找匹配的问题  
            for query, answer in self.naive_answers.items():
                if question in query or self._questions_match(question, query):
                    naive_answer = answer
                    break
            
            # 如果两个模式都有答案，添加到答案对中
            if hybrid_answer and naive_answer:
                length_diff = abs(len(hybrid_answer) - len(naive_answer))
                answer_pairs.append({
                    "question": question,
                    "hybrid_answer": hybrid_answer,
                    "naive_answer": naive_answer,
                    "length_diff": length_diff
                })
        
        print(f"✅ 找到 {len(answer_pairs)} 个Hybrid vs Naive答案对")
        return answer_pairs
    
    def _questions_match(self, q1, q2, threshold=0.8):
        """简单的问题匹配算法"""
        # 将问题转换为小写并去除标点
        q1_clean = re.sub(r'[^\w\s]', '', q1.lower())
        q2_clean = re.sub(r'[^\w\s]', '', q2.lower())
        
        # 计算词汇重叠率
        words1 = set(q1_clean.split())
        words2 = set(q2_clean.split())
        
        if len(words1) == 0 or len(words2) == 0:
            return False
            
        overlap = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return overlap / union >= threshold
    
    async def test_position_bias_hybrid_naive(self, answer_pairs):
        """测试Hybrid vs Naive的位置偏置"""
        
        judge_prompt_template = """
请评判以下两个答案哪个更好地回答了问题。请从准确性、完整性、清晰度等方面进行评估：

问题：{question}

答案A：{answer_a}

答案B：{answer_b}

请选择更好的答案（A或B）并简要说明理由：
"""
        
        position_bias_results = []
        
        print(f"开始测试所有 {len(answer_pairs)} 个答案对的位置偏置...")
        
        for pair_idx, pair in enumerate(answer_pairs):
            print(f"\n测试答案对 {pair_idx + 1}/{len(answer_pairs)}: {pair['question'][:100]}...")
            
            # 测试 Hybrid(A) vs Naive(B)
            prompt_hybrid_first = judge_prompt_template.format(
                question=pair["question"],
                answer_a=pair["hybrid_answer"],
                answer_b=pair["naive_answer"]
            )
            
            # 测试 Naive(A) vs Hybrid(B) - 交换位置
            prompt_naive_first = judge_prompt_template.format(
                question=pair["question"],
                answer_a=pair["naive_answer"],
                answer_b=pair["hybrid_answer"]
            )
            
            try:
                # 测试Hybrid在A位置
                result_hybrid_first = await lightrag_llm_func_async(
                    prompt_hybrid_first,
                    system="你是一个公正的评判者，请客观评价答案质量。",
                    max_tokens=300,
                    temperature=0.1
                )
                
                await asyncio.sleep(1)  # 避免请求过快
                
                # 测试Naive在A位置
                result_naive_first = await lightrag_llm_func_async(
                    prompt_naive_first,
                    system="你是一个公正的评判者，请客观评价答案质量。",
                    max_tokens=300,
                    temperature=0.1
                )
                
                # 解析结果
                hybrid_first_choice = self.parse_judge_result(result_hybrid_first)
                naive_first_choice = self.parse_judge_result(result_naive_first)
                
                # 转换为实际选择的模式
                if hybrid_first_choice == "A":
                    choice_when_hybrid_first = "hybrid"
                elif hybrid_first_choice == "B":
                    choice_when_hybrid_first = "naive"
                else:
                    choice_when_hybrid_first = "unclear"
                
                if naive_first_choice == "A":
                    choice_when_naive_first = "naive"
                elif naive_first_choice == "B":
                    choice_when_naive_first = "hybrid"
                else:
                    choice_when_naive_first = "unclear"
                
                # 检查一致性
                consistent = choice_when_hybrid_first == choice_when_naive_first
                
                result_record = {
                    "pair_id": pair_idx,
                    "question": pair["question"][:100],
                    "choice_when_hybrid_first": choice_when_hybrid_first,
                    "choice_when_naive_first": choice_when_naive_first,
                    "consistent": consistent,
                    "length_diff": pair["length_diff"],
                    "hybrid_length": len(pair["hybrid_answer"]),
                    "naive_length": len(pair["naive_answer"])
                }
                
                position_bias_results.append(result_record)
                
                if not consistent:
                    print(f"🚨 位置偏置检测: 答案对{pair_idx+1}")
                    print(f"  Hybrid在前选择: {choice_when_hybrid_first}")
                    print(f"  Naive在前选择: {choice_when_naive_first}")
                else:
                    print(f"✅ 一致选择: {choice_when_hybrid_first}")
                
                await asyncio.sleep(1)  # 控制请求频率
                
            except Exception as e:
                print(f"❌ 位置偏置测试失败 (答案对{pair_idx+1}): {e}")
                await asyncio.sleep(2)
        
        self.results["position_bias"] = position_bias_results
        return position_bias_results
    
    async def test_length_bias_hybrid_naive(self, answer_pairs):
        """测试Hybrid vs Naive的长度偏置"""
        
        judge_prompt_template = """
请评判以下两个答案哪个更好地回答了问题：

问题：{question}

答案A：{answer_a}

答案B：{answer_b}

请选择更好的答案（A或B）并简要说明理由：
"""
        
        length_bias_results = []
        
        # 选择长度差异较大的答案对
        long_diff_pairs = [p for p in answer_pairs if p["length_diff"] >= 100]
        print(f"测试 {len(long_diff_pairs)} 个长度差异显著的答案对...")
        
        for pair in long_diff_pairs:
            # 确定长短答案
            if len(pair["hybrid_answer"]) > len(pair["naive_answer"]):
                long_answer = pair["hybrid_answer"]
                short_answer = pair["naive_answer"]
                long_mode = "hybrid"
                short_mode = "naive"
            else:
                long_answer = pair["naive_answer"]
                short_answer = pair["hybrid_answer"]
                long_mode = "naive"
                short_mode = "hybrid"
            
            print(f"\n测试长度偏置: {short_mode}({len(short_answer)}字) vs {long_mode}({len(long_answer)}字)")
            
            # 测试短答案在前
            prompt_short_first = judge_prompt_template.format(
                question=pair["question"],
                answer_a=short_answer,
                answer_b=long_answer
            )
            
            # 测试长答案在前
            prompt_long_first = judge_prompt_template.format(
                question=pair["question"],
                answer_a=long_answer,
                answer_b=short_answer
            )
            
            try:
                # 短答案在前
                result_short_first = await lightrag_llm_func_async(
                    prompt_short_first,
                    system="你是一个公正的评判者，请客观评价答案质量。",
                    max_tokens=300,
                    temperature=0.1
                )
                
                await asyncio.sleep(1)
                
                # 长答案在前
                result_long_first = await lightrag_llm_func_async(
                    prompt_long_first,
                    system="你是一个公正的评判者，请客观评价答案质量。",
                    max_tokens=300,
                    temperature=0.1
                )
                
                choice_short_first = self.parse_judge_result(result_short_first)
                choice_long_first = self.parse_judge_result(result_long_first)
                
                # 判断是否选择了更长的答案
                chose_longer_when_short_first = choice_short_first == "B"
                chose_longer_when_long_first = choice_long_first == "A"
                
                length_bias_results.append({
                    "question": pair["question"][:100],
                    "short_mode": short_mode,
                    "long_mode": long_mode,
                    "chose_longer_when_short_first": chose_longer_when_short_first,
                    "chose_longer_when_long_first": chose_longer_when_long_first,
                    "length_ratio": len(long_answer) / len(short_answer),
                    "short_length": len(short_answer),
                    "long_length": len(long_answer)
                })
                
                print(f"  短在前选择: {'长答案' if chose_longer_when_short_first else '短答案'}")
                print(f"  长在前选择: {'长答案' if chose_longer_when_long_first else '短答案'}")
                
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"❌ 长度偏置测试失败: {e}")
                await asyncio.sleep(2)
        
        self.results["length_bias"] = length_bias_results
        return length_bias_results
    
    async def test_trial_bias_hybrid_naive(self, answer_pairs):
        """测试试次偏置（多次评判同一对答案的一致性）"""
        
        judge_prompt_template = """
请评判以下两个答案哪个更好地回答了问题。请从准确性、完整性、清晰度等方面进行评估：

问题：{question}

答案A：{answer_a}

答案B：{answer_b}

请选择更好的答案（A或B）并简要说明理由：
"""
        
        trial_bias_results = []
        
        # 选择部分答案对进行多次试验
        test_pairs = random.sample(answer_pairs, min(5, len(answer_pairs)))  # 限制测试数量
        trials_per_pair = 3  # 每对答案测试3次
        
        print(f"开始测试试次偏置: {len(test_pairs)} 个答案对，每对测试 {trials_per_pair} 次...")
        
        for pair_idx, pair in enumerate(test_pairs):
            print(f"\n测试答案对 {pair_idx + 1}/{len(test_pairs)}: {pair['question'][:100]}...")
            
            pair_results = {
                "pair_id": pair_idx,
                "question": pair["question"][:100],
                "hybrid_length": len(pair["hybrid_answer"]),
                "naive_length": len(pair["naive_answer"]),
                "trials": []
            }
            
            # 进行多次试验
            for trial_num in range(trials_per_pair):
                print(f"  试验 {trial_num + 1}/{trials_per_pair}...")
                
                # 随机选择答案顺序（避免位置偏置影响试次偏置测试）
                if random.random() < 0.5:
                    # Hybrid在前
                    prompt = judge_prompt_template.format(
                        question=pair["question"],
                        answer_a=pair["hybrid_answer"],
                        answer_b=pair["naive_answer"]
                    )
                    order = "hybrid_first"
                else:
                    # Naive在前
                    prompt = judge_prompt_template.format(
                        question=pair["question"],
                        answer_a=pair["naive_answer"],
                        answer_b=pair["hybrid_answer"]
                    )
                    order = "naive_first"
                
                try:
                    result = await lightrag_llm_func_async(
                        prompt,
                        system="你是一个公正的评判者，请客观评价答案质量。",
                        max_tokens=300,
                        temperature=0.3  # 稍微提高温度以观察变化
                    )
                    
                    choice = self.parse_judge_result(result)
                    
                    # 将选择转换为实际选择的模式
                    if order == "hybrid_first":
                        actual_choice = "hybrid" if choice == "A" else "naive" if choice == "B" else "unclear"
                    else:
                        actual_choice = "naive" if choice == "A" else "hybrid" if choice == "B" else "unclear"
                    
                    pair_results["trials"].append({
                        "trial_num": trial_num + 1,
                        "order": order,
                        "raw_choice": choice,
                        "actual_choice": actual_choice
                    })
                    
                    print(f"    选择: {actual_choice}")
                    await asyncio.sleep(2)  # 控制请求频率
                    
                except Exception as e:
                    print(f"    ❌ 试验失败: {e}")
                    await asyncio.sleep(3)
            
            # 分析该答案对的试次一致性
            actual_choices = [t["actual_choice"] for t in pair_results["trials"] if t["actual_choice"] != "unclear"]
            
            if len(actual_choices) >= 2:
                # 计算一致性
                hybrid_count = actual_choices.count("hybrid")
                naive_count = actual_choices.count("naive")
                
                consistency = max(hybrid_count, naive_count) / len(actual_choices)
                dominant_choice = "hybrid" if hybrid_count > naive_count else "naive" if naive_count > hybrid_count else "tie"
                
                pair_results.update({
                    "total_valid_trials": len(actual_choices),
                    "hybrid_count": hybrid_count,
                    "naive_count": naive_count,
                    "consistency_rate": consistency,
                    "dominant_choice": dominant_choice,
                    "has_trial_bias": consistency < 0.8  # 一致性低于80%认为有试次偏置
                })
                
                if consistency < 0.8:
                    print(f"  🚨 检测到试次偏置! 一致性: {consistency:.2%}")
                else:
                    print(f"  ✅ 试次一致性良好: {consistency:.2%}")
            
            trial_bias_results.append(pair_results)
        
        self.results["trial_bias"] = trial_bias_results
        return trial_bias_results
    
    async def test_hybrid_vs_naive_preference(self, answer_pairs):
        """测试对Hybrid vs Naive的整体偏好"""
        
        judge_prompt_template = """
请评判以下两个RAG系统回答哪个更好：

问题：{question}

Hybrid模式回答：{hybrid_answer}

Naive模式回答：{naive_answer}

请选择更好的回答（Hybrid或Naive）并说明理由：
"""
        
        preference_results = []
        
        print(f"测试所有 {len(answer_pairs)} 个答案对的模式偏好...")
        
        for pair_idx, pair in enumerate(answer_pairs[:10]):  # 限制数量避免过多请求
            print(f"\n测试模式偏好 {pair_idx + 1}/10: {pair['question'][:100]}...")
            
            prompt = judge_prompt_template.format(
                question=pair["question"],
                hybrid_answer=pair["hybrid_answer"],
                naive_answer=pair["naive_answer"]
            )
            
            try:
                result = await lightrag_llm_func_async(
                    prompt,
                    system="你是一个公正的评判者，请客观评价RAG系统的回答质量。",
                    max_tokens=300,
                    temperature=0.1
                )
                
                # 解析选择了哪个模式
                if "hybrid" in result.lower() or "混合" in result.lower():
                    choice = "hybrid"
                elif "naive" in result.lower() or "朴素" in result.lower():
                    choice = "naive"
                else:
                    choice = "unclear"
                
                preference_results.append({
                    "pair_id": pair_idx,
                    "question": pair["question"][:100],
                    "choice": choice,
                    "hybrid_length": len(pair["hybrid_answer"]),
                    "naive_length": len(pair["naive_answer"]),
                    "length_diff": pair["length_diff"]
                })
                
                print(f"选择: {choice}")
                await asyncio.sleep(1)
                
            except Exception as e:
                print(f"❌ 模式偏好测试失败: {e}")
                await asyncio.sleep(2)
        
        self.results["hybrid_vs_naive_comparison"] = preference_results
        return preference_results
    
    def parse_judge_result(self, result):
        """解析LLM评判结果"""
        result_lower = result.lower()
        
        if "答案a" in result_lower or "选择a" in result_lower or "a更好" in result_lower or "a比较好" in result_lower:
            return "A"
        elif "答案b" in result_lower or "选择b" in result_lower or "b更好" in result_lower or "b比较好" in result_lower:
            return "B"
        else:
            # 尝试其他解析方式
            a_count = result.count("A") + result.count("a")
            b_count = result.count("B") + result.count("b")
            
            if a_count > b_count:
                return "A"
            elif b_count > a_count:
                return "B"
            else:
                return "UNCLEAR"
    
    async def run_comprehensive_bias_analysis(self, results_dir, questions_file):
        """运行全面的偏置分析（位置、长度、试次）"""
        print("🧪 开始全面偏置分析（位置 + 长度 + 试次）")
        
        # 加载真实数据
        if not self.load_real_data(results_dir, questions_file):
            print("❌ 数据加载失败，无法进行分析")
            return None
        
        # 找到所有Hybrid vs Naive答案对
        answer_pairs = self.find_all_hybrid_naive_pairs()
        if not answer_pairs:
            print("❌ 没有找到合适的答案对")
            return None
        
        print("\n1️⃣ 测试位置偏置...")
        await self.test_position_bias_hybrid_naive(answer_pairs)
        
        print("\n2️⃣ 测试长度偏置...")
        await self.test_length_bias_hybrid_naive(answer_pairs)
        
        print("\n3️⃣ 测试试次偏置...")
        await self.test_trial_bias_hybrid_naive(answer_pairs)
        
        print("\n4️⃣ 测试模式偏好...")
        await self.test_hybrid_vs_naive_preference(answer_pairs)
        
        return self.results
    
    def generate_comprehensive_bias_report(self):
        """生成全面的偏置分析报告"""
        print("\n" + "="*70)
        print("🔍 全面偏置分析报告")
        print("="*70)
        
        # 位置偏置报告
        if self.results["position_bias"]:
            position_data = self.results["position_bias"]
            consistent_count = sum(1 for item in position_data if item["consistent"])
            consistency_rate = consistent_count / len(position_data) if position_data else 0
            
            print(f"\n📍 位置偏置分析:")
            print(f"  总测试答案对: {len(position_data)}")
            print(f"  判断一致性: {consistency_rate:.2%}")
            print(f"  偏置程度: {(1-consistency_rate)*100:.1f}%")
            
            if consistency_rate < 0.7:
                print("  ⚠️  检测到严重位置偏置！")
            else:
                print("  ✅  位置偏置在可接受范围内")
        
        # 长度偏置报告
        if self.results["length_bias"]:
            length_data = self.results["length_bias"]
            total_tests = len(length_data) * 2
            chose_longer_count = sum(1 for item in length_data if item["chose_longer_when_short_first"]) + \
                               sum(1 for item in length_data if item["chose_longer_when_long_first"])
            longer_preference = chose_longer_count / total_tests if total_tests > 0 else 0
            
            print(f"\n📏 长度偏置分析:")
            print(f"  测试答案对数: {len(length_data)}")
            print(f"  偏好长答案比例: {longer_preference:.2%}")
            print(f"  长度偏置程度: {abs(longer_preference - 0.5)*200:.1f}%")
            
            if longer_preference > 0.7 or longer_preference < 0.3:
                print("  ⚠️  检测到明显的长度偏置！")
            else:
                print("  ✅  长度偏置在可接受范围内")
        
        # 试次偏置报告
        if self.results["trial_bias"]:
            trial_data = self.results["trial_bias"]
            total_pairs = len(trial_data)
            biased_pairs = sum(1 for item in trial_data if item.get("has_trial_bias", False))
            
            # 计算平均一致性
            consistency_rates = [item.get("consistency_rate", 0) for item in trial_data if "consistency_rate" in item]
            avg_consistency = sum(consistency_rates) / len(consistency_rates) if consistency_rates else 0
            
            print(f"\n🔄 试次偏置分析:")
            print(f"  测试答案对数: {total_pairs}")
            print(f"  平均试次一致性: {avg_consistency:.2%}")
            print(f"  存在试次偏置的答案对: {biased_pairs} ({biased_pairs/total_pairs*100:.1f}%)")
            
            if avg_consistency < 0.7:
                print("  ⚠️  检测到严重的试次偏置！")
            elif biased_pairs / total_pairs > 0.3:
                print("  ⚠️  检测到中等程度的试次偏置")
            else:
                print("  ✅  试次偏置在可接受范围内")
        
        # 模式偏好报告
        if self.results["hybrid_vs_naive_comparison"]:
            preference_data = self.results["hybrid_vs_naive_comparison"]
            hybrid_count = sum(1 for item in preference_data if item["choice"] == "hybrid")
            naive_count = sum(1 for item in preference_data if item["choice"] == "naive")
            
            print(f"\n🔄 模式偏好分析:")
            print(f"  偏好Hybrid: {hybrid_count} ({hybrid_count/len(preference_data)*100:.1f}%)")
            print(f"  偏好Naive: {naive_count} ({naive_count/len(preference_data)*100:.1f}%)")
        
        # 综合偏置评估
        print(f"\n📊 综合偏置评估:")
        bias_score = 0
        
        # 位置偏置评分
        position_data = self.results.get("position_bias", [])
        if position_data:
            consistent_count = sum(1 for item in position_data if item["consistent"])
            consistency_rate = consistent_count / len(position_data)
            
            if consistency_rate < 0.7:
                bias_score += 30
                print("  - 位置偏置: 严重 (+30分)")
            elif consistency_rate < 0.8:
                bias_score += 15
                print("  - 位置偏置: 中等 (+15分)")
            else:
                print("  - 位置偏置: 轻微 (+0分)")
        
        # 长度偏置评分
        length_data = self.results.get("length_bias", [])
        if length_data:
            total_tests = len(length_data) * 2
            chose_longer_count = sum(1 for item in length_data if item["chose_longer_when_short_first"]) + \
                               sum(1 for item in length_data if item["chose_longer_when_long_first"])
            longer_preference = chose_longer_count / total_tests if total_tests > 0 else 0.5
            
            if longer_preference > 0.7 or longer_preference < 0.3:
                bias_score += 25
                print("  - 长度偏置: 严重 (+25分)")
            elif longer_preference > 0.6 or longer_preference < 0.4:
                bias_score += 10
                print("  - 长度偏置: 中等 (+10分)")
            else:
                print("  - 长度偏置: 轻微 (+0分)")
        
        # 试次偏置评分
        trial_data = self.results.get("trial_bias", [])
        if trial_data:
            consistency_rates = [item.get("consistency_rate", 0) for item in trial_data if "consistency_rate" in item]
            avg_consistency = sum(consistency_rates) / len(consistency_rates) if consistency_rates else 1.0
            biased_pairs = sum(1 for item in trial_data if item.get("has_trial_bias", False))
            total_pairs = len(trial_data)
            
            if avg_consistency < 0.7:
                bias_score += 20
                print("  - 试次偏置: 严重 (+20分)")
            elif biased_pairs / total_pairs > 0.3:
                bias_score += 10
                print("  - 试次偏置: 中等 (+10分)")
            else:
                print("  - 试次偏置: 轻微 (+0分)")
        
        print(f"\n🎯 总偏置分数: {bias_score}/75")
        if bias_score >= 50:
            print("  ❌ 评判系统存在严重偏置，需要重新设计")
        elif bias_score >= 25:
            print("  ⚠️  评判系统存在中等偏置，建议改进")
        else:
            print("  ✅ 评判系统偏置可接受")
        
        # 改进建议
        print(f"\n💡 改进建议:")
        if any(consistency_rate < 0.8 for consistency_rate in [consistency_rate] if position_data):
            print("  - 实施双向评判策略减少位置偏置")
        if any(longer_preference > 0.6 or longer_preference < 0.4 for longer_preference in [longer_preference] if length_data):
            print("  - 在评判指令中强调质量而非长度")
        if any(avg_consistency < 0.8 for avg_consistency in [avg_consistency] if trial_data):
            print("  - 降低temperature参数提高评判一致性")
            print("  - 使用多次评判并取众数结果")
        print("  - 使用盲评方式隐藏模式信息")
        print("  - 考虑使用专门训练的评判模型")
    
    def save_results(self, output_file):
        """保存分析结果"""
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        
        # 生成详细报告
        self.generate_comprehensive_bias_report()

async def main():
    """主函数"""
    analyzer = JudgeBiasAnalyzer()
    
    # 设置数据路径
    results_dir = "../exp_results/data/results"
    questions_file = "../exp_results/data/questions/cs_questions.txt"
    
    # 运行全面偏置分析（包含三种偏置测试）
    results = await analyzer.run_comprehensive_bias_analysis(results_dir, questions_file)
    
    if results:
        # 保存结果
        output_file = "../exp_results/data/evaluations/comprehensive_bias_analysis.json"
        analyzer.save_results(output_file)
        
        print(f"\n💾 全面偏置分析结果已保存到: {output_file}")
    else:
        print("❌ 偏置分析失败")

if __name__ == "__main__":
    asyncio.run(main())