import os
import json
import argparse

def create_simple_contexts(corpus_directory):
    """只使用passages.jsonl创建简单上下文，限制数量和长度"""
    print(f"📊 从 {corpus_directory} 加载passages数据...")
    
    # 只加载passages
    passages_file = os.path.join(corpus_directory, "passages.jsonl")
    if not os.path.exists(passages_file):
        print(f"❌ 找不到 {passages_file}")
        return []
    
    contexts = []
    with open(passages_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                passage = json.loads(line)
                text = passage['text'].strip()
                
                # 限制长度：只取前1000字符，避免过长
                if len(text) > 1000:
                    text = text[:1000] + "..."
                
                # 跳过太短的段落
                if len(text) > 100:
                    contexts.append(text)
    
    print(f"✅ 提取了 {len(contexts)} 个段落")
    
    # 限制总数量，只取前50个最重要的段落
    if len(contexts) > 50:
        print(f"⚠️ 段落数量过多({len(contexts)})，只保留前50个")
        contexts = contexts[:50]
    
    return contexts

def main():
    parser = argparse.ArgumentParser(description="创建简化的LightRAG上下文")
    parser.add_argument(
        "-i", "--input_dir", 
        type=str, 
        default="/mnt/jfs/wangpengyu/UltraWikiDomain/corpus",
        help="UltraWikiDomain corpus 目录路径"
    )
    parser.add_argument(
        "-o", "--output_dir", 
        type=str, 
        default="../exp_results/data/unique_contexts",
        help="输出目录路径"
    )
    
    args = parser.parse_args()
    
    corpus_dir = args.input_dir
    output_dir = args.output_dir
    
    os.makedirs(output_dir, exist_ok=True)
    
    # 创建简单上下文
    contexts = create_simple_contexts(corpus_dir)
    
    if not contexts:
        print("❌ 没有提取到任何上下文")
        return
    
    # 去重
    unique_contexts = []
    seen = set()
    for ctx in contexts:
        if ctx not in seen:
            unique_contexts.append(ctx)
            seen.add(ctx)
    
    print(f"📊 去重后: {len(unique_contexts)} 个唯一段落")
    
    # 统计信息
    total_chars = sum(len(ctx) for ctx in unique_contexts)
    avg_chars = total_chars / len(unique_contexts) if unique_contexts else 0
    print(f"📊 平均长度: {avg_chars:.0f} 字符")
    print(f"📊 总字符数: {total_chars:,}")
    
    # 保存
    CLASS = os.getenv("CLASS", "cs")
    output_file = os.path.join(output_dir, f"{CLASS}_unique_contexts.json")
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(unique_contexts, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 保存到: {output_file}")
    print(f"🎯 预计插入时间: 2-5分钟")

if __name__ == "__main__":
    main()