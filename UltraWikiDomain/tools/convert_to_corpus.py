import os, re, json, hashlib, itertools, pathlib, pandas as pd
from datetime import datetime
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse
import glob

def sha1(x: str) -> str:
    return hashlib.sha1(x.encode('utf-8', 'ignore')).hexdigest()

def word_count(s): 
    return len(re.findall(r"\w+", s))

def norm_url(u: str) -> str:
    if not u: return None
    u = re.sub(r'#.*$', '', u)               # 去掉fragment
    u = re.sub(r'([\?&])(utm_[^=]+=[^&]+)&?', r'\1', u) # 去UTM
    u = u.rstrip('?&')
    return u

def normalize_title_for_matching(title):
    """将标题标准化用于文件名匹配"""
    if not title:
        return ""
    
    # 转为小写
    normalized = title.lower()
    
    # 移除或替换标点符号
    normalized = re.sub(r"[''']", "", normalized)  # 移除撇号
    normalized = re.sub(r'[^\w\s]', ' ', normalized)  # 其他标点符号替换为空格
    
    # 标准化空格
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

def md_to_text(md: str) -> str:
    """极简 markdown -> 文本转换"""
    # 去掉YAML front matter
    if md.startswith('---'):
        lines = md.split('\n')
        end_idx = 1
        for i, line in enumerate(lines[1:], 1):
            if line.strip() == '---':
                end_idx = i + 1
                break
        md = '\n'.join(lines[end_idx:])
    
    txt = re.sub(r'```.*?```', '', md, flags=re.S)       # 代码块
    txt = re.sub(r'`[^`]+`', '', txt)                    # 行内代码
    txt = re.sub(r'!\[[^\]]*\]\([^)]+\)', '', txt)       # 图片
    txt = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', txt)   # 链接文本
    txt = re.sub(r'[#>*_`]+', ' ', txt)                  # 标记符
    txt = re.sub(r'\|[^|\n]*\|', '', txt)                # 表格
    txt = re.sub(r'\s+\n', '\n', txt)
    return re.sub(r'[ \t]+', ' ', txt).strip()

def split_sentences(text: str):
    """句子切分"""
    sentences = re.split(r'(?<=[\.!?])\s+(?=[A-Z0-9])', text)
    return [s.strip() for s in sentences if s.strip()]

def chunk_passages(sents, max_words=350):
    """将句子切块为passages"""
    def wc(s): return len(re.findall(r'\w+', s))
    buf, words, out = [], 0, []
    for sent in sents:
        w = wc(sent)
        if buf and words + w > max_words:
            out.append(" ".join(buf))
            buf, words = [], 0
        buf.append(sent)
        words += w
    if buf: 
        out.append(" ".join(buf))
    return out

def classify_source_type(url: str) -> str:
    """根据URL分类来源类型"""
    if not url:
        return "web"
    
    domain = urlparse(url).netloc.lower()
    
    # 政府网站
    if any(gov in domain for gov in ['.gov', '.mil', '.int']):
        return "gov"
    
    # 教育机构
    if '.edu' in domain:
        return "edu"
    
    # 新闻网站
    news_domains = ['bbc.com', 'cnn.com', 'reuters.com', 'nytimes.com', 
                   'washingtonpost.com', 'guardian.com', 'npr.org', 'baltimoresun.com']
    if any(news in domain for news in news_domains):
        return "news"
    
    # 学术期刊
    journal_domains = ['pubmed.ncbi.nlm.nih.gov', 'doi.org', 'jstor.org', 
                      'springer.com', 'nature.com', 'science.org']
    if any(journal in domain for journal in journal_domains):
        return "journal"
    
    # 书籍
    book_domains = ['books.google.com', 'archive.org']
    if any(book in domain for book in book_domains):
        return "book"
    
    return "web"

def find_markdown_file(basedir):
    """在目录中查找markdown文件"""
    md_files = []
    for file in os.listdir(basedir):
        if file.endswith('.md'):
            md_files.append(os.path.join(basedir, file))
    
    if not md_files:
        return None
    
    # 优先选择主文件
    dir_name = os.path.basename(basedir)
    
    for md_file in md_files:
        filename = os.path.basename(md_file)
        if filename == f"{dir_name}.md" or filename == "wiki.md":
            return md_file
    
    return md_files[0]

def process_external_references(basedir, doc_id):
    """处理外部参考文献"""
    ref_file = os.path.join(basedir, "reference", "references.jsonl")
    ref_pages_dir = os.path.join(basedir, "reference", "reference_pages")
    
    print(f"  检查参考文献文件: {ref_file}")
    print(f"  检查参考页面目录: {ref_pages_dir}")
    print(f"  references.jsonl存在: {os.path.exists(ref_file)}")
    print(f"  reference_pages目录存在: {os.path.exists(ref_pages_dir)}")
    
    if not os.path.exists(ref_file):
        print(f"  Warning: {ref_file} not found")
        return [], [], []
    
    refs = []
    with open(ref_file, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if line:
                try:
                    refs.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"    Warning: JSON解析失败在第{line_num}行: {e}")
                    continue
    
    print(f"  读取到 {len(refs)} 个引用")
    
    if not os.path.exists(ref_pages_dir):
        print(f"  Warning: {ref_pages_dir} not found")
        # 仍然创建ext_docs，但没有全文
        ext_docs = []
        ref2ext = []
        for idx, r in enumerate(refs, start=1):
            nu = norm_url(r.get("url"))
            if nu:
                ext_doc_id = f"ext_{sha1(nu)[:16]}"
                ext_docs.append({
                    "ext_doc_id": ext_doc_id,
                    "url": r.get("url"),
                    "norm_url": nu,
                    "archive_url": r.get("archive_url"),
                    "title": r.get("title"),
                    "authors": r.get("author"),
                    "source": r.get("source"),
                    "publish_date": r.get("publish_date"),
                    "retrieved_date": r.get("retrieved_date"),
                    "source_type": classify_source_type(r.get("url")),
                    "lang": "en",
                    "status": "unknown",
                    "has_fulltext": False,
                    "n_tokens": None,
                    "sha1_text": None,
                })
                ref2ext.append({
                    "doc_id": doc_id,
                    "ref_id": f"R{idx}",
                    "ext_doc_id": ext_doc_id
                })
        return ext_docs, [], ref2ext
    
    # 列出reference_pages目录内容
    ref_files = os.listdir(ref_pages_dir)
    print(f"  reference_pages目录包含 {len(ref_files)} 个文件")
    md_files = [f for f in ref_files if f.endswith('.md')]
    print(f"  其中 {len(md_files)} 个.md文件")
    
    # 构建文件名到路径的映射，并创建标准化的文件名映射
    file_map = {}
    normalized_file_map = {}
    
    for file in md_files:
        file_path = os.path.join(ref_pages_dir, file)
        file_map[file] = file_path
        
        # 创建标准化的文件名（去掉.md后缀并标准化）
        file_title = file.replace('.md', '')
        normalized_file_title = normalize_title_for_matching(file_title)
        normalized_file_map[normalized_file_title] = file_path
    
    print(f"  可用的抓取文件:")
    for i, (norm_name, path) in enumerate(list(normalized_file_map.items())):
        print(f"    {i+1}. '{norm_name}' -> {os.path.basename(path)}")
    
    ext_docs = []
    ext_passages = []
    ref2ext = []
    
    processed_files = 0
    matched_files = 0
    
    for idx, r in enumerate(refs, start=1):
        nu = norm_url(r.get("url"))
        if not nu:
            continue
        
        # 每个引用都有唯一的ext_doc_id，不去重
        ext_doc_id = f"ext_{sha1(nu)[:16]}"
        
        # 只进行100%精确匹配
        page_file = None
        title = r.get('title', '')
        
        if title:
            # 标准化引用标题
            normalized_title = normalize_title_for_matching(title)
            
            # 只有100%精确匹配才接受
            if normalized_title in normalized_file_map:
                page_file = normalized_file_map[normalized_title]
                matched_files += 1
                print(f"    #{idx} 精确匹配: '{title}' -> {os.path.basename(page_file)}")
            else:
                # 调试：显示最接近的匹配（但不使用）
                print(f"    #{idx} 无精确匹配: '{title}' (标准化: '{normalized_title}')")
        
        has_full = page_file is not None
        text = None
        
        if has_full:
            try:
                with open(page_file, "r", encoding="utf-8", errors="ignore") as f:
                    md = f.read()
                
                if not md.strip():
                    print(f"    文件为空: {os.path.basename(page_file)}")
                    has_full = False
                else:
                    text = md_to_text(md)
                    word_cnt = word_count(text)
                    
                    # 降低质量门槛到50词
                    if word_cnt >= 50:
                        sents = split_sentences(text)
                        chunks = chunk_passages(sents, max_words=350)
                        
                        for pidx, ch in enumerate(chunks):
                            ext_passages.append({
                                "ext_passage_id": f"{ext_doc_id}_{pidx:05d}",
                                "ext_doc_id": ext_doc_id,
                                "text": ch,
                                "start_char": 0,
                                "end_char": len(ch),
                                "n_tokens": word_count(ch),
                                "sha1_text": sha1(ch),
                            })
                        processed_files += 1
                        print(f"    处理成功: {word_cnt}词 -> {len(chunks)}个chunks")
                    else:
                        print(f"    文件质量不达标，只有{word_cnt}词")
                        has_full = False
                        text = None
            except Exception as e:
                print(f"    处理文件失败 {os.path.basename(page_file)}: {e}")
                has_full = False
                text = None
        
        # 每个引用都创建一个ext_doc条目
        ext_docs.append({
            "ext_doc_id": ext_doc_id,
            "url": r.get("url"),
            "norm_url": nu,
            "archive_url": r.get("archive_url"),
            "title": r.get("title"),
            "authors": r.get("author"),
            "source": r.get("source"),
            "publish_date": r.get("publish_date"),
            "retrieved_date": r.get("retrieved_date"),
            "source_type": classify_source_type(r.get("url")),
            "lang": "en",
            "status": "200" if has_full else "unknown",
            "has_fulltext": has_full,
            "n_tokens": (word_count(text) if text else None),
            "sha1_text": (sha1(text) if text else None),
        })
        
        # 建立ref到ext的映射
        ref2ext.append({
            "doc_id": doc_id,
            "ref_id": f"R{idx}",
            "ext_doc_id": ext_doc_id
        })
    
    print(f"  精确匹配统计: {matched_files}/{len(refs)} 个引用找到了100%匹配的文件")
    print(f"  成功处理 {processed_files} 个外部文档文件")
    print(f"  生成 {len(ext_docs)} 个ext_docs, {len(ext_passages)} 个ext_passages")
    
    return ext_docs, ext_passages, ref2ext

def extract_claims(doc_id, sentences, ref_mentions, ext_passages_by_ref):
    """提取claims（可选）"""
    claims = []
    
    # 找到带有引用的句子
    ref_by_sent = defaultdict(list)
    for mention in ref_mentions:
        sent_idx = mention.get('sent_idx')
        if sent_idx is not None:
            ref_by_sent[sent_idx].append(mention['ref_id'])
    
    for sent_idx, refs in ref_by_sent.items():
        if sent_idx < len(sentences):
            sentence = sentences[sent_idx]
            claim_text = sentence['text']
            
            # 为每个引用找到最相关的ext_passage（简化：取第一个）
            for ref_id in refs:
                if ref_id in ext_passages_by_ref:
                    ext_passages = ext_passages_by_ref[ref_id]
                    if ext_passages:
                        # 简化：取第一个passage作为支撑证据
                        ext_passage_id = ext_passages[0]['ext_passage_id']
                        ext_doc_id = ext_passages[0]['ext_doc_id']
                        
                        claims.append({
                            "claim_id": f"{doc_id}_claim_{sent_idx}_{ref_id}",
                            "doc_id": doc_id,
                            "sent_idx": sent_idx,
                            "claim_text": claim_text,
                            "ref_id": ref_id,
                            "ext_doc_id": ext_doc_id,
                            "ext_passage_id": ext_passage_id,
                            "label": "support"
                        })
    
    return claims

def process_single_wiki(basedir):
    """处理单个wiki文章目录"""
    print(f"Processing: {basedir}")
    
    # --- 读入原始 ---
    wiki_file = find_markdown_file(basedir)
    if wiki_file is None:
        print(f"Warning: No markdown file found in {basedir}, skipping")
        return None
    
    print(f"  Found markdown file: {os.path.basename(wiki_file)}")
        
    try:
        with open(wiki_file, "r", encoding="utf-8", errors="ignore") as f:
            md = f.read()
    except Exception as e:
        print(f"Error reading {wiki_file}: {e}")
        return None

    # --- 轻量清洗 ---
    md = md.replace('\r\n', '\n')
    
    # 去掉YAML front matter（如果存在）
    if md.startswith('---'):
        lines = md.split('\n')
        end_idx = 1
        for i, line in enumerate(lines[1:], 1):
            if line.strip() == '---':
                end_idx = i + 1
                break
        md = '\n'.join(lines[end_idx:])
    
    # 找到"参考文献"起点
    ref_sec_pat = re.compile(r'^\s{0,3}#{1,6}\s*(References|Notes|Bibliography|Citations)\s*$', re.I | re.M)
    m = ref_sec_pat.search(md)
    body = md[:m.start()] if m else md

    # 标题/分节
    sec_pat = re.compile(r'^(#{1,6})\s+(.*)$', re.M)
    sections = []
    sec_id = -1
    for m in sec_pat.finditer(body):
        if sec_id >= 0:
            sections[-1]['end_char'] = m.start()
        sec_id += 1
        sections.append({
            'section_id': sec_id,
            'heading': m.group(2).strip(),
            'level': len(m.group(1)),
            'start_char': m.end(),
            'end_char': None
        })
    
    if sections:


        
        sections[-1]['end_char'] = len(body)
    else:
        sections = [{'section_id':0,'heading':'','level':1,'start_char':0,'end_char':len(body)}]

    # 句子切分
    sent_split = re.compile(r'(?<=[\.!?])\s+(?=[A-Z0-9])')
    sentences = []
    
    for sec in sections:
        sec_text = body[sec['start_char']:sec['end_char']]
        start = sec['start_char']
        parts = []
        last = 0
        for mm in sent_split.finditer(sec_text):
            parts.append((last, mm.end()))
            last = mm.end()
        parts.append((last, len(sec_text)))
        
        for (a,b) in parts:
            txt = sec_text[a:b].strip()
            if not txt: 
                continue
            global_a = start + a
            global_b = start + b
            sentences.append({
                'section_id': sec['section_id'],
                'start_char': global_a,
                'end_char': global_b,
                'text': txt
            })

    # passages 切块
    passages = []
    buf = []
    buf_words = 0
    pidx = 0
    for i, s in enumerate(sentences):
        w = word_count(s['text'])
        if buf and buf_words + w > 350:
            start_char = buf[0]['start_char']
            end_char = buf[-1]['end_char']
            text = " ".join(x['text'] for x in buf)
            passages.append({
                'passage_id': f"doc_{pidx:05d}",
                'section_id': buf[0]['section_id'],
                'text': text,
                'start_char': start_char,
                'end_char': end_char,
                'sent_start_idx': i - len(buf),
                'sent_end_idx': i - 1,
                'n_tokens': word_count(text),
                'sha1_text': sha1(text)
            })
            pidx += 1
            buf, buf_words = [], 0
        buf.append(s)
        buf_words += w
    
    if buf:
        start_char = buf[0]['start_char']
        end_char = buf[-1]['end_char']
        text = " ".join(x['text'] for x in buf)
        passages.append({
            'passage_id': f"doc_{pidx:05d}",
            'section_id': buf[0]['section_id'],
            'text': text,
            'start_char': start_char,
            'end_char': end_char,
            'sent_start_idx': len(sentences) - len(buf),
            'sent_end_idx': len(sentences) - 1,
            'n_tokens': word_count(text),
            'sha1_text': sha1(text)
        })

    # wikilinks
    link_pat = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
    wikilinks = []
    for m in link_pat.finditer(body):
        anchor, url = m.group(1), m.group(2)
        if ('wikipedia.org/wiki/' in url) or url.startswith('/wiki/'):
            target_url = url
            target_title = (url.split('/wiki/')[-1]).split('#')[0]
            import urllib.parse
            target_title = urllib.parse.unquote(target_title).replace('_', ' ')
            wikilinks.append({
                'anchor_text': anchor,
                'anchor_start_char': m.start(),
                'anchor_end_char': m.end(),
                'target_title': target_title,
                'target_url': target_url
            })

    # 生成临时doc_id用于处理references
    doc_text = body
    temp_doc_id = f"wiki_en_{sha1(doc_text)[:16]}"
    
    # 处理外部参考文献
    ext_docs, ext_passages, ref2ext = process_external_references(basedir, temp_doc_id)
    
    # references & ref_mentions
    refs = []
    ref_file = os.path.join(basedir, "reference", "references.jsonl")
    if os.path.exists(ref_file):
        with open(ref_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        refs.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    
    references = []
    for idx, r in enumerate(refs, start=1):
        references.append({
            'ref_id': f"R{idx}",
            'title': r.get('title'),
            'url': r.get('url'),
            'norm_url': norm_url(r.get('url')),
            'is_external': r.get('is_external'),
            'author': r.get('author'),
            'source': r.get('source'),
            'archive_url': r.get('archive_url'),
            'publish_date': r.get('publish_date'),
            'retrieved_date': r.get('retrieved_date'),
            'scraped': r.get('scraped'),
            'ref_hash': sha1("|".join(str(x or "") for x in [
                r.get('title'), r.get('publish_date'), r.get('url')
            ]))
        })

    # 把 [1] / [^1] 之类映射到 ref_id - 改进引用匹配逻辑
    ref_mentions = []
    # 匹配多种引用格式：[1], [^1], [[1]], (1), etc.
    cite_patterns = [
        re.compile(r'\[\[(\d+)\]\]'),  # [[1]]
        re.compile(r'\[(\d+)\]'),     # [1]
        re.compile(r'\[\^(\d+)\]'),   # [^1]
        re.compile(r'\((\d+)\)'),     # (1)
    ]
    
    for si, s in enumerate(sentences):
        for pattern in cite_patterns:
            for m in pattern.finditer(s['text']):
                try:
                    num = int(m.group(1))
                    if 1 <= num <= len(references):
                        ref_mentions.append({
                            'ref_id': f"R{num}",
                            'section_id': s['section_id'],
                            'sent_idx': si,
                            'anchor_offset_start': m.start(),
                            'anchor_offset_end': m.end()
                        })
                except ValueError:
                    continue

    # 构建ext_passages按ref_id的索引（用于claims）
    ext_passages_by_ref = defaultdict(list)
    for ref_mapping in ref2ext:
        ref_id = ref_mapping['ref_id']
        ext_doc_id = ref_mapping['ext_doc_id']
        for passage in ext_passages:
            if passage['ext_doc_id'] == ext_doc_id:
                ext_passages_by_ref[ref_id].append(passage)

    # 提取claims（可选）
    claims = extract_claims(temp_doc_id, sentences, ref_mentions, ext_passages_by_ref)

    # docs汇总
    title_match = re.match(r'^\s*#\s*(.+)$', md, re.M)
    title = title_match.group(1).strip() if title_match else os.path.basename(wiki_file).replace('.md', '')
    
    doc_info = {
        'doc_id': temp_doc_id,
        'title': title,
        'url': None,
        'n_tokens': word_count(doc_text),
        'n_sentences': len(sentences),
        'n_refs': len(references),
        'sha1_text': sha1(doc_text),
        'source_dir': basedir,
        'source_file': wiki_file
    }

    return {
        'doc': doc_info,
        'sections': sections,
        'sentences': sentences,
        'passages': passages,
        'wikilinks': wikilinks,
        'references': references,
        'ref_mentions': ref_mentions,
        'ext_docs': ext_docs,
        'ext_passages': ext_passages,
        'ref2ext': ref2ext,
        'claims': claims
    }

def main():
    """主函数：处理raw/目录下的所有wiki文章"""
    raw_dir = "/mnt/jfs/wangpengyu/UltraWikiDomain/raw"
    
    if not os.path.exists(raw_dir):
        print(f"Error: {raw_dir} directory not found")
        return
    
    # 收集所有结果
    all_docs = []
    all_sections = []
    all_sentences = []
    all_passages = []
    all_wikilinks = []
    all_references = []
    all_ref_mentions = []
    all_ext_docs = []
    all_ext_passages = []
    all_ref2ext = []
    all_claims = []
    
    # 全局计数器
    global_doc_idx = 0
    global_passage_idx = 0
    global_sentence_idx = 0
    global_section_idx = 0
    global_ext_passage_idx = 0
    
    # 用于去重ext_docs
    seen_ext_docs = {}
    
    # 遍历raw/目录下的所有子目录
    for item in sorted(os.listdir(raw_dir)):
        item_path = os.path.join(raw_dir, item)
        if os.path.isdir(item_path):
            try:
                result = process_single_wiki(item_path)
                if result is None:
                    continue
                    
                # 更新doc ID
                doc = result['doc']
                doc['doc_id'] = f"wiki_en_{global_doc_idx:05d}_{doc['sha1_text'][:8]}"
                doc['global_doc_idx'] = global_doc_idx
                all_docs.append(doc)
                
                # 更新其他表的doc_id
                for sec in result['sections']:
                    sec['global_section_id'] = global_section_idx
                    sec['doc_id'] = doc['doc_id']
                    all_sections.append(sec)
                    global_section_idx += 1
                
                for sent in result['sentences']:
                    sent['global_sent_id'] = global_sentence_idx
                    sent['doc_id'] = doc['doc_id']
                    all_sentences.append(sent)
                    global_sentence_idx += 1
                
                for passage in result['passages']:
                    passage['passage_id'] = f"doc_{global_passage_idx:05d}"
                    passage['doc_id'] = doc['doc_id']
                    all_passages.append(passage)
                    global_passage_idx += 1
                
                for link in result['wikilinks']:
                    link['doc_id'] = doc['doc_id']
                    all_wikilinks.append(link)
                
                for ref in result['references']:
                    ref['doc_id'] = doc['doc_id']
                    all_references.append(ref)
                
                for mention in result['ref_mentions']:
                    mention['doc_id'] = doc['doc_id']
                    all_ref_mentions.append(mention)
                
                # 处理外部文档（去重）
                for ext_doc in result['ext_docs']:
                    ext_doc_id = ext_doc['ext_doc_id']
                    if ext_doc_id not in seen_ext_docs:
                        seen_ext_docs[ext_doc_id] = ext_doc
                        all_ext_docs.append(ext_doc)
                
                # 外部passages
                for ext_passage in result['ext_passages']:
                    ext_passage['global_ext_passage_id'] = global_ext_passage_idx
                    all_ext_passages.append(ext_passage)
                    global_ext_passage_idx += 1
                
                # ref2ext映射
                for mapping in result['ref2ext']:
                    mapping['doc_id'] = doc['doc_id']
                    all_ref2ext.append(mapping)
                
                # claims
                for claim in result['claims']:
                    claim['doc_id'] = doc['doc_id']
                    all_claims.append(claim)
                
                global_doc_idx += 1
                
                # 打印统计
                print(f"  - {doc['title'] or 'Untitled'}")
                print(f"    sections: {len(result['sections'])}, passages: {len(result['passages'])}")
                print(f"    wikilinks: {len(result['wikilinks'])}, refs: {len(result['references'])}")
                print(f"    ext_docs: {len(result['ext_docs'])}, ext_passages: {len(result['ext_passages'])}")
                print(f"    claims: {len(result['claims'])}")
                
            except Exception as e:
                print(f"Error processing {item_path}: {e}")
                import traceback
                traceback.print_exc()
                continue
    
    # 打印总体统计
    print("\n=== 总体统计 ===")
    print(f"总文档数: {len(all_docs)}")
    print(f"总章节数: {len(all_sections)}")
    print(f"总句子数: {len(all_sentences)}")
    print(f"总段落数: {len(all_passages)}")
    print(f"总链接数: {len(all_wikilinks)}")
    print(f"总引用数: {len(all_references)}")
    print(f"总引用提及: {len(all_ref_mentions)}")
    print(f"总外部文档数: {len(all_ext_docs)}")
    print(f"总外部段落数: {len(all_ext_passages)}")
    print(f"总ref2ext映射: {len(all_ref2ext)}")
    print(f"总claims: {len(all_claims)}")
    
    # 保存到parquet格式
    output_dir = "/mnt/jfs/wangpengyu/UltraWikiDomain/corpus"
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存为 parquet 和 jsonl 两种格式
    formats = ['parquet', 'jsonl']
    
    for fmt in formats:
        try:
            if fmt == 'parquet':
                # 原有的6张表
                pd.DataFrame(all_docs).to_parquet(os.path.join(output_dir, "docs.parquet"), compression='snappy')
                pd.DataFrame(all_sections).to_parquet(os.path.join(output_dir, "sections.parquet"), compression='snappy')
                pd.DataFrame(all_sentences).to_parquet(os.path.join(output_dir, "sentences.parquet"), compression='snappy')
                pd.DataFrame(all_passages).to_parquet(os.path.join(output_dir, "passages.parquet"), compression='snappy')
                if all_wikilinks:
                    pd.DataFrame(all_wikilinks).to_parquet(os.path.join(output_dir, "wikilinks.parquet"), compression='snappy')
                if all_references:
                    pd.DataFrame(all_references).to_parquet(os.path.join(output_dir, "references.parquet"), compression='snappy')
                if all_ref_mentions:
                    pd.DataFrame(all_ref_mentions).to_parquet(os.path.join(output_dir, "ref_mentions.parquet"), compression='snappy')
                
                # 新增的外部数据表
                if all_ext_docs:
                    pd.DataFrame(all_ext_docs).to_parquet(os.path.join(output_dir, "ext_docs.parquet"), compression='snappy')
                if all_ext_passages:
                    pd.DataFrame(all_ext_passages).to_parquet(os.path.join(output_dir, "ext_passages.parquet"), compression='snappy')
                if all_ref2ext:
                    pd.DataFrame(all_ref2ext).to_parquet(os.path.join(output_dir, "ref2ext.parquet"), compression='snappy')
                if all_claims:
                    pd.DataFrame(all_claims).to_parquet(os.path.join(output_dir, "claims.parquet"), compression='snappy')
            
            elif fmt == 'jsonl':
                # 保存 JSONL 格式
                def save_jsonl(data, filename):
                    if data:
                        with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
                            for item in data:
                                f.write(json.dumps(item, ensure_ascii=False) + '\n')
                
                save_jsonl(all_docs, "docs.jsonl")
                save_jsonl(all_sections, "sections.jsonl")
                save_jsonl(all_sentences, "sentences.jsonl")
                save_jsonl(all_passages, "passages.jsonl")
                save_jsonl(all_wikilinks, "wikilinks.jsonl")
                save_jsonl(all_references, "references.jsonl")
                save_jsonl(all_ref_mentions, "ref_mentions.jsonl")
                save_jsonl(all_ext_docs, "ext_docs.jsonl")
                save_jsonl(all_ext_passages, "ext_passages.jsonl")
                save_jsonl(all_ref2ext, "ref2ext.jsonl")
                save_jsonl(all_claims, "claims.jsonl")
        
        except Exception as e:
            print(f"Error saving {fmt} format: {e}")
    
    print(f"\n结果已保存到 {output_dir}/ 目录")

if __name__ == "__main__":
    main()