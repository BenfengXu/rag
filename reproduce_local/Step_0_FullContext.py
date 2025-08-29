import os
import json
import glob
import argparse
from collections import defaultdict


def load_jsonl_data(file_path):
    """加载 JSONL 文件数据"""
    data = []
    if not os.path.exists(file_path):
        return data
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:
                    try:
                        data.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"JSON解析错误 {file_path} 第{line_num}行: {e}")
    except Exception as e:
        print(f"读取文件错误 {file_path}: {e}")
    
    return data


def create_enhanced_contexts(corpus_directory):
    """基于所有11个表创建增强的上下文"""
    print(f"📊 从 {corpus_directory} 加载所有数据表...")
    
    # 1. 加载所有数据表
    data_tables = {}
    table_names = [
        'docs', 'sections', 'sentences', 'passages', 'wikilinks', 
        'references', 'ref_mentions', 'ext_docs', 'ext_passages', 
        'ref2ext', 'claims'
    ]
    
    for table_name in table_names:
        file_path = os.path.join(corpus_directory, f"{table_name}.jsonl")
        data_tables[table_name] = load_jsonl_data(file_path)
        print(f"  ✅ {table_name}: {len(data_tables[table_name])} 条记录")
    
    # 2. 构建索引用于快速查找
    print("🔗 构建数据索引...")
    
    # 文档索引
    docs_by_id = {doc['doc_id']: doc for doc in data_tables['docs']}
    
    # 章节索引
    sections_by_doc = defaultdict(list)
    for section in data_tables['sections']:
        sections_by_doc[section['doc_id']].append(section)
    
    # 句子索引
    sentences_by_doc = defaultdict(list)
    sentences_by_section = defaultdict(list)
    for sentence in data_tables['sentences']:
        sentences_by_doc[sentence['doc_id']].append(sentence)
        sentences_by_section[sentence['section_id']].append(sentence)
    
    # 引用索引
    refs_by_doc = defaultdict(list)
    for ref in data_tables['references']:
        refs_by_doc[ref['doc_id']].append(ref)
    
    # 引用提及索引
    ref_mentions_by_doc = defaultdict(list)
    for mention in data_tables['ref_mentions']:
        ref_mentions_by_doc[mention['doc_id']].append(mention)
    
    # 维基链接索引
    wikilinks_by_doc = defaultdict(list)
    for link in data_tables['wikilinks']:
        wikilinks_by_doc[link['doc_id']].append(link)
    
    # 外部文档索引
    ext_docs_by_id = {ext_doc['ext_doc_id']: ext_doc for ext_doc in data_tables['ext_docs']}
    
    # 外部段落索引
    ext_passages_by_doc = defaultdict(list)
    for ext_passage in data_tables['ext_passages']:
        ext_passages_by_doc[ext_passage['ext_doc_id']].append(ext_passage)
    
    # ref2ext 映射
    ext_docs_by_ref = {}
    for mapping in data_tables['ref2ext']:
        ext_docs_by_ref[mapping['ref_id']] = mapping['ext_doc_id']
    
    # Claims 索引
    claims_by_doc = defaultdict(list)
    for claim in data_tables['claims']:
        claims_by_doc[claim['doc_id']].append(claim)
    
    # 3. 创建增强的上下文
    contexts = []
    
    print("🚀 创建增强上下文...")
    
    # 3.1 主文档段落（增强版）
    for passage in data_tables['passages']:
        doc_id = passage['doc_id']
        doc_info = docs_by_id.get(doc_id, {})
        
        # 找到相关的句子
        passage_sentences = [
            s for s in sentences_by_doc[doc_id]
            if (s.get('start_char', 0) >= passage.get('start_char', 0) and 
                s.get('end_char', 0) <= passage.get('end_char', 0))
        ]
        
        # 找到相关的引用提及
        passage_ref_mentions = [
            rm for rm in ref_mentions_by_doc[doc_id]
            if any(s.get('global_sent_id') == rm.get('sent_idx') for s in passage_sentences)
        ]
        
        # 找到相关的维基链接
        passage_wikilinks = [
            wl for wl in wikilinks_by_doc[doc_id]
            if (wl.get('anchor_start_char', 0) >= passage.get('start_char', 0) and 
                wl.get('anchor_end_char', 0) <= passage.get('end_char', 0))
        ]
        
        # 找到相关的claims
        passage_claims = [
            c for c in claims_by_doc[doc_id]
            if any(s.get('global_sent_id') == c.get('sent_idx') for s in passage_sentences)
        ]
        
        # 构建增强的上下文
        enhanced_text = passage['text']
        
        # 可选：添加引用信息到文本中
        if passage_ref_mentions:
            ref_info = []
            for rm in passage_ref_mentions:
                ref_id = rm['ref_id']
                # 找到对应的引用详情
                ref_detail = next((r for r in refs_by_doc[doc_id] if r['ref_id'] == ref_id), None)
                if ref_detail and ref_detail.get('title'):
                    ref_info.append(f"[{ref_id}: {ref_detail['title']}]")
            
            if ref_info:
                enhanced_text += f"\n\nReferences: {'; '.join(ref_info)}"
        
        # 可选：添加维基链接信息
        if passage_wikilinks:
            entity_info = [f"{wl['anchor_text']} -> {wl['target_title']}" for wl in passage_wikilinks]
            if entity_info:
                enhanced_text += f"\n\nLinked Entities: {'; '.join(entity_info)}"
        
        context = {
            "context": enhanced_text,
            "id": passage['passage_id'],
            "type": "main_passage",
            "metadata": {
                "doc_id": doc_id,
                "doc_title": doc_info.get('title'),
                "section_id": passage.get('section_id'),
                "n_tokens": passage.get('n_tokens'),
                "n_sentences": len(passage_sentences),
                "n_references": len(passage_ref_mentions),
                "n_wikilinks": len(passage_wikilinks),
                "n_claims": len(passage_claims),
                "reference_ids": [rm['ref_id'] for rm in passage_ref_mentions],
                "linked_entities": [wl['target_title'] for wl in passage_wikilinks],
                "has_claims": len(passage_claims) > 0
            }
        }
        contexts.append(context)
    
    # 3.2 外部文档段落（增强版）
    for ext_passage in data_tables['ext_passages']:
        ext_doc_id = ext_passage['ext_doc_id']
        ext_doc_info = ext_docs_by_id.get(ext_doc_id, {})
        
        # 找到引用这个外部文档的主文档
        referencing_refs = [ref_id for ref_id, ext_id in ext_docs_by_ref.items() if ext_id == ext_doc_id]
        
        # 构建外部文档的增强文本
        enhanced_text = ext_passage['text']
        
        # 添加外部文档的元信息
        if ext_doc_info.get('title'):
            enhanced_text = f"Source: {ext_doc_info['title']}\n\n{enhanced_text}"
        
        if ext_doc_info.get('source'):
            enhanced_text += f"\n\nPublished by: {ext_doc_info['source']}"
        
        context = {
            "context": enhanced_text,
            "id": ext_passage['ext_passage_id'],
            "type": "external_passage",
            "metadata": {
                "ext_doc_id": ext_doc_id,
                "title": ext_doc_info.get('title'),
                "source": ext_doc_info.get('source'),
                "source_type": ext_doc_info.get('source_type'),
                "url": ext_doc_info.get('url'),
                "authors": ext_doc_info.get('authors'),
                "publish_date": ext_doc_info.get('publish_date'),
                "n_tokens": ext_passage.get('n_tokens'),
                "referenced_by": referencing_refs,
                "has_fulltext": ext_doc_info.get('has_fulltext', False)
            }
        }
        contexts.append(context)
    
    # 3.3 Claims（作为独立的上下文）
    for claim in data_tables['claims']:
        doc_id = claim['doc_id']
        doc_info = docs_by_id.get(doc_id, {})
        
        # 找到相关的外部文档信息
        ext_doc_id = claim.get('ext_doc_id')
        ext_doc_info = ext_docs_by_id.get(ext_doc_id, {}) if ext_doc_id else {}
        
        enhanced_text = claim['claim_text']
        
        # 添加支撑证据信息
        if ext_doc_info.get('title'):
            enhanced_text += f"\n\nSupported by: {ext_doc_info['title']}"
            if ext_doc_info.get('source'):
                enhanced_text += f" ({ext_doc_info['source']})"
        
        context = {
            "context": enhanced_text,
            "id": claim['claim_id'],
            "type": "claim",
            "metadata": {
                "doc_id": doc_id,
                "doc_title": doc_info.get('title'),
                "ref_id": claim.get('ref_id'),
                "ext_doc_id": ext_doc_id,
                "ext_passage_id": claim.get('ext_passage_id'),
                "label": claim.get('label'),
                "sent_idx": claim.get('sent_idx'),
                "has_evidence": bool(ext_doc_id)
            }
        }
        contexts.append(context)
    
    # 3.4 文档级别的上下文（基于sections）
    for doc_id, doc_sections in sections_by_doc.items():
        doc_info = docs_by_id.get(doc_id, {})
        
        # 为每个主要章节创建概览上下文
        for section in doc_sections:
            if section.get('level', 1) <= 2:  # 只处理1-2级标题
                section_sentences = sentences_by_section.get(section['section_id'], [])
                section_text = ' '.join([s['text'] for s in section_sentences[:3]])  # 取前3句作为概览
                
                if len(section_text.strip()) > 50:  # 确保有足够的内容
                    context = {
                        "context": f"Section: {section['heading']}\n\n{section_text}...",
                        "id": f"section_{section['section_id']}",
                        "type": "section_overview",
                        "metadata": {
                            "doc_id": doc_id,
                            "doc_title": doc_info.get('title'),
                            "section_id": section['section_id'],
                            "heading": section['heading'],
                            "level": section['level'],
                            "n_sentences": len(section_sentences)
                        }
                    }
                    contexts.append(context)
    
    print(f"  ✅ 创建了 {len(contexts)} 个增强上下文")
    
    # 统计信息
    type_counts = defaultdict(int)
    for ctx in contexts:
        type_counts[ctx['type']] += 1
    
    print("📊 上下文类型统计:")
    for ctx_type, count in type_counts.items():
        print(f"  {ctx_type}: {count}")
    
    return contexts


def extract_unique_contexts(input_directory, output_directory):
    """处理 UltraWikiDomain corpus 数据"""
    os.makedirs(output_directory, exist_ok=True)
    
    print(f"🚀 处理 UltraWikiDomain corpus: {input_directory}")
    
    # 检查是否是 corpus 目录
    required_files = ['passages.jsonl', 'ext_passages.jsonl', 'docs.jsonl']
    missing_files = []
    for file_name in required_files:
        if not os.path.exists(os.path.join(input_directory, file_name)):
            missing_files.append(file_name)
    
    if missing_files:
        print(f"❌ 缺少必需文件: {missing_files}")
        print("请确保输入目录是 UltraWikiDomain corpus 目录")
        return
    
    # 创建增强的上下文
    contexts = create_enhanced_contexts(input_directory)
    
    if not contexts:
        print("❌ 没有创建任何上下文")
        return
    
    # 去重
    unique_contexts_dict = {}
    for ctx in contexts:
        context_text = ctx['context']
        if context_text and context_text not in unique_contexts_dict:
            unique_contexts_dict[context_text] = ctx
    
    unique_contexts_list = list(unique_contexts_dict.keys())
    unique_contexts_with_metadata = list(unique_contexts_dict.values())
    
    print(f"📊 去重后: {len(unique_contexts_list)} 个唯一上下文")
    
    # 保存结果
    CLASS = os.getenv("CLASS", "ultrawiki")
    output_filename = f"{CLASS}_unique_contexts.json"
    output_path = os.path.join(output_directory, output_filename)
    
    try:
        with open(output_path, "w", encoding="utf-8") as outfile:
            json.dump(unique_contexts_list, outfile, ensure_ascii=False, indent=2)
        print(f"✅ 唯一上下文已保存到: {output_filename}")
    except Exception as e:
        print(f"❌ 保存文件时出错: {e}")
        return
    
    # 保存带元数据的完整版本
    metadata_filename = "ultrawiki_contexts_with_metadata.json"
    metadata_path = os.path.join(output_directory, metadata_filename)
    
    try:
        with open(metadata_path, "w", encoding="utf-8") as outfile:
            json.dump(unique_contexts_with_metadata, outfile, ensure_ascii=False, indent=2)
        print(f"✅ 带元数据的上下文已保存到: {metadata_filename}")
    except Exception as e:
        print(f"❌ 保存元数据文件时出错: {e}")
    
    # 创建 LightRAG 需要的格式
    lightrag_filename = "ultrawiki.jsonl"
    lightrag_path = os.path.join(output_directory, lightrag_filename)
    
    try:
        with open(lightrag_path, "w", encoding="utf-8") as outfile:
            for ctx in unique_contexts_with_metadata:
                lightrag_record = {
                    "context": ctx["context"],
                    "id": ctx["id"],
                    "type": ctx["type"],
                    "metadata": ctx["metadata"]
                }
                outfile.write(json.dumps(lightrag_record, ensure_ascii=False) + '\n')
        print(f"✅ LightRAG格式已保存到: {lightrag_filename}")
    except Exception as e:
        print(f"❌ 保存LightRAG格式时出错: {e}")
    
    print("🎉 所有文件处理完成!")


if __name__ == "__main__":
    CLASS = os.getenv("CLASS", "ultrawiki")
    parser = argparse.ArgumentParser(description="处理 UltraWikiDomain corpus 数据")
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

    extract_unique_contexts(args.input_dir, args.output_dir)