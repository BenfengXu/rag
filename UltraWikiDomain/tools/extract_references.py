"""引用提取脚本

功能:
1. 使用 jina_scraping 中的 WebScrapingJinaTool 抓取网页并保存 Markdown（形成 目录/标题/reference/ 结构）
2. 从保存的 Markdown 中解析 References 段落里的引用链接
3. 生成 reference/references.jsonl，每行一个引用项，包含：
   - url
   - is_external (是否是外链)
   - jumpup (若包含脚注“↑”或"jump up"等标记则给出对象信息，否则为空字符串)
   - title (引用标题估计)
   - date (若能解析出日期)

说明:
由于 Jina 抓取返回的是经过提炼的文本而非原始 HTML，本脚本采用启发式解析，可能不能 100% 复原复杂的 Wikipedia 引用结构；可按需后续改为直接用 MediaWiki API 获取更结构化数据。
"""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import List, Dict, Any, Optional

from jina_scraping import WebScrapingJinaTool, save_markdown, DEFAULT_API_KEY  # type: ignore


# URL 提取正则：匹配 http/https 开头直到遇到空白或右括号/引号/方括号
URL_REGEX = re.compile(r'(https?://[^\s\)\]\><"\']+)')
DATE_ISO_REGEX = re.compile(r'\b(\d{4}-\d{2}-\d{2})\b')
DATE_YMD_REGEX = re.compile(r'\b(\d{4})[\./年-](\d{1,2})[\./月-](\d{1,2})日?\b')
MONTH_NAME_REGEX = re.compile(r'\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}\b')
RETRIEVED_DATE_REGEX = re.compile(r'(Retrieved\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4})\.?', re.IGNORECASE)
# Jump/脚注模式（窄化：仅去除单个 **[^](...)** 或 ^ 及紧随的 anchor 链接，而不吞掉后续正文）
JUMP_CARET_PATTERN = re.compile(r'(\*\*\[\^]\([^)]*\)\*\*)')  # **[^](...)**
JUMP_INLINE_ANCHORS_PATTERN = re.compile(r'(\[[^\]]+\]\(https?://[^)]+cite_ref[^)]*\))')  # a,b,c锚点
JUMP_PHRASE_PATTERN = re.compile(r'^\s*\^?\s*Jump up to:?', re.IGNORECASE)
MD_LINK_REGEX = re.compile(r'\[([^\]]+)\]\((https?://[^)]+)\)')


def parse_references_block(markdown_text: str) -> List[str]:
    """定位参考文献区块和Bibliography区块并返回其中的原始行。

    支持以下结构示例:
    1. References\n----------\n### Citations\n<entries>
    2. # References / ## References 形式
    3. Bibliography / ## Bibliography 形式  
    4. 页面末尾的无标题引用列表（Tell es-Sakan模式）
    5. 编号引用列表（1. ^ Jump up to: ... 模式）
    """
    lines = markdown_text.splitlines()
    n = len(lines)
    ref_start_idx: Optional[int] = None
    bib_start_idx: Optional[int] = None
    citations_anchor_idx: Optional[int] = None
    numbered_refs_start: Optional[int] = None

    for i, ln in enumerate(lines):
        stripped = ln.strip()
        low = stripped.lower()
        
        # 查找 References 部分
        if low in {"references", "参考文献"}:
            ref_start_idx = i
            if i + 1 < n and re.fullmatch(r'-{3,}', lines[i+1].strip()):
                pass
        # 直接 markdown 形式标题 - References
        if low.startswith('#') and 'references' in low:
            ref_start_idx = i
            
        # 查找 Bibliography 部分
        if low in {"bibliography", "书目", "参考书目"}:
            bib_start_idx = i
            if i + 1 < n and re.fullmatch(r'-{3,}', lines[i+1].strip()):
                pass
        # 直接 markdown 形式标题 - Bibliography
        if low.startswith('#') and 'bibliography' in low:
            bib_start_idx = i
            
        if '### citations' in low:
            citations_anchor_idx = i
            if ref_start_idx is None:
                ref_start_idx = i
        
        # 查找编号引用列表的开始（如 "1. ^ Jump up to:" 或 "1. **^**"）
        if numbered_refs_start is None and re.match(r'^\s*1\.\s*[\^\*]*\s*(Jump up|[\*\^])', stripped, re.IGNORECASE):
            numbered_refs_start = i

    # 收集所有相关内容
    collected: List[str] = []
    
    # 处理 References 部分
    if ref_start_idx is not None:
        start_collect = (citations_anchor_idx + 1) if citations_anchor_idx is not None else (ref_start_idx + 1)
        end_collect = min(
            x for x in [bib_start_idx, numbered_refs_start, n] 
            if x is not None and x > ref_start_idx
        )
        
        for j in range(start_collect, end_collect):
            ln = lines[j]
            stripped = ln.strip()
            if re.match(r'^#{1,3} ', stripped):
                low = stripped.lower()
                if not ('reference' in low or 'citation' in low or 'bibliography' in low):
                    break
            if stripped.lower().startswith(('external links', 'see also', 'notes')):
                if 'bibliography' not in stripped.lower():
                    break
            collected.append(ln)
    
    # 处理 Bibliography 部分
    if bib_start_idx is not None:
        start_collect = bib_start_idx + 1
        end_collect = numbered_refs_start if numbered_refs_start and numbered_refs_start > bib_start_idx else n
        
        for j in range(start_collect, end_collect):
            ln = lines[j]
            stripped = ln.strip()
            if re.match(r'^#{1,3} ', stripped):
                low = stripped.lower()
                if not ('bibliography' in low or 'reference' in low or 'citation' in low):
                    break
            if stripped.lower().startswith(('external links', 'see also', 'notes')):
                break
            collected.append(ln)
    
    # 处理编号引用列表
    if numbered_refs_start is not None:
        for j in range(numbered_refs_start, n):
            ln = lines[j]
            stripped = ln.strip()
            
            # 如果是编号引用格式，继续收集
            if re.match(r'^\s*\d+\.\s*[\^\*]*\s*(Jump up|\*)', stripped, re.IGNORECASE):
                collected.append(ln)
            # 如果是空行，跳过
            elif not stripped:
                collected.append(ln)
            # 如果遇到新的章节标题，停止
            elif re.match(r'^#{1,3} ', stripped):
                break
            # 如果遇到明显的其他内容，停止
            elif stripped.lower().startswith(('external links', 'see also', 'notes')):
                break
            # 否则认为是引用的延续部分
            else:
                collected.append(ln)
    
    # 如果没有找到正式的References/Bibliography段，也没有编号引用，查找页面末尾的引用列表
    if ref_start_idx is None and bib_start_idx is None and numbered_refs_start is None:
        # 原有的末尾引用检测逻辑
        ref_lines_from_end = []
        for i in range(n-1, -1, -1):
            line = lines[i].strip()
            if not line:
                continue
            if line.startswith('*') and '[' in line and '](' in line:
                ref_lines_from_end.insert(0, lines[i])
            elif 'wikimedia' in line.lower() and '[' in line:
                ref_lines_from_end.insert(0, lines[i])
            elif line.startswith('#') or 'edit section' in line.lower():
                break
            elif not line.startswith('*') and '[' not in line and len(line) > 20:
                break
        
        if ref_lines_from_end:
            collected.extend(ref_lines_from_end)
    
    return collected
def group_reference_entries(ref_lines: List[str]) -> List[str]:
    """将引用区块按空行或编号/列表起始分组，输出每条引用文本。"""
    entries: List[str] = []
    buffer: List[str] = []
    def flush():
        if buffer:
            # 合并并压缩空白
            merged = ' '.join(l.strip() for l in buffer if l.strip())
            if merged:
                entries.append(merged)
            buffer.clear()
    for ln in ref_lines:
        stripped = ln.strip()
        if not stripped:
            flush()
            continue
        # 典型编号形式 [1] 或 1. 或 - 号开头，新条目
        if re.match(r'^\s*(\[[0-9]+\]|[0-9]+[.)]|[-*])\s+', ln):
            flush()
            buffer.append(stripped)
        else:
            buffer.append(stripped)
    flush()
    return entries


def extract_date(text: str) -> Optional[str]:
    # 优先匹配 Retrieved 格式 (包含尾部句点)
    m = RETRIEVED_DATE_REGEX.search(text)
    if m:
        phrase = m.group(1)
        # 确保以句点结束
        return phrase if phrase.endswith('.') else phrase + '.'
    # ISO
    m = DATE_ISO_REGEX.search(text)
    if m:
        return m.group(1)
    # YYYY-MM-DD 或带中文分隔
    m = DATE_YMD_REGEX.search(text)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    # Month name (出版日期) 直接返回
    m = MONTH_NAME_REGEX.search(text)
    if m:
        return m.group(0)
    return None


def build_reference_items(ref_entries: List[str]) -> List[Dict[str, Any]]:
    """解析引用条目 -> 结构化字段。

     新规则汇总（来自 ref_cases.md）：
     1. 去除所有 "Jump up" / a b c 脚注跳转锚点 (包含 cite_ref / cite_note / caret ^ 形式)。
     2. 若首个“正文”链接是同页锚点 (#CITEREF / #cite_ref) 且整条中不存在任何外链或 Archive 链接，则整条丢弃 (纯页内书目/Works cited 指向)。
     3. 标题 = 第一条 *内容* 链接文本 (剥离引号 / 前后空白)。
         - 若该链接 URL 仍是 wikipedia.org 且存在 [Archived](archive_url) 链接，则标题仍取第一条，最终 url 取 Archived 链接。
    4. 若存在 [Archived](archive_url)，保留 archive_url 字段，但最终抓取使用原始非 Archived 链接 (url 字段)；不再替换为归档地址。
     5. 作者：
         - 有 (Month Day, Year) 出版日期：日期前的非空文本（去除跳转锚点与多余标点）。
         - 无出版日期：若在第一条标题链接之前出现以句点结束的短文本 (<= 12 词)，视为作者。例如："United States Congress."。
     6. sources：收集标题之后的所有非 'Archived' 链接文本；包括媒体名、出版物、ISSN/ISBN 及其编号链接；保持去重顺序。若只有 1 个 source 仍放在列表里。
     7. 过滤：
         - 没有任何可抓取 url（既无外部 http(s) 链接，亦无归档链接）=> 丢弃 (如纯书目：_Promises to Keep: ..._ 无链接)。
         - 仅含内部 #CITEREF/#cite_ref 链接 => 丢弃。
     8. 归一化：
         - 标题去除首尾中文/英文引号、强调符号 _ *。
         - 去掉重复空白。
     9. 日期：
         - publish_date：第一个 (Month Day, Year) 样式。
         - retrieved_date：'Retrieved Month Day, Year'。
    10. 仍保留 is_external：指最终使用的 url 是否外部 (非 wikipedia.org)。
    11. 作者为空时，若 sources 存在，作者回填为第一个 source（机构即作者）。
    """
    items: List[Dict[str, Any]] = []
    month_names = r'(January|February|March|April|May|June|July|August|September|October|November|December)'
    publish_date_pat = re.compile(r'\((' + month_names + r')\s+\d{1,2},\s+\d{4}\)')
    retrieved_pat = re.compile(r'Retrieved\s+' + month_names + r'\s+\d{1,2},\s+\d{4}\.?', re.IGNORECASE)

    def is_jump_token(text: str, url: str) -> bool:
        t = text.lower().strip('_* \'"')
        if 'jump' in t:  # Jump up / Jump up to
            return True
        # cite_ref anchors + single-letter label (a,b,c, etc.)
        if 'cite_ref' in url and re.fullmatch(r'[a-z]', t):
            return True
        # caret marker
        if t in {'^'}:
            return True
        return False

    for raw_entry in ref_entries:
        entry = raw_entry.strip()
        if not entry:
            continue
        # 去除行首编号 "1."、"[1]"、"-" 等
        entry = re.sub(r'^\s*(\[[0-9]+\]|[0-9]+[.)]|[-*])\s+', '', entry)
        # 去除开头 Jump up phrase
        entry = JUMP_PHRASE_PATTERN.sub('', entry)
        # 去除 caret jump 组件 **[^](...)**
        entry = JUMP_CARET_PATTERN.sub(' ', entry)
        # 去除 cite_ref 锚点链接集合 (a,b,c...)
        entry = JUMP_INLINE_ANCHORS_PATTERN.sub(' ', entry)
        entry = re.sub(r'\s+', ' ', entry).strip()

        md_links = MD_LINK_REGEX.findall(entry)
        if not md_links:
            # 没有任何链接 => 可能是书籍（无 url） -> 丢弃
            continue

        # 过滤掉纯脚注/跳转/单字母锚点
        def is_pure_anchor(txt: str, url: str) -> bool:
            t = txt.strip().lower().strip('_*"')
            if not url:
                return True
            if 'cite_ref' in url or 'cite_note' in url:
                return True
            if re.match(r'^[a-z]$', t):
                return True
            if 'jump' in t:
                return True
            return False

        content_links = [(t, u) for (t, u) in md_links if not is_pure_anchor(t, u)]
        if not content_links:
            continue

        # 识别出版日期 & Retrieved
        m_pub = publish_date_pat.search(entry)
        m_ret = retrieved_pat.search(entry)
        publish_date = m_pub.group(0).strip('()') if m_pub else ''
        retrieved_date = m_ret.group(0) if m_ret else ''
        if retrieved_date and not retrieved_date.endswith('.'):
            retrieved_date += '.'

        def clean_name(s: str) -> str:
            s = re.sub(r'\s+', ' ', s).strip()
            # 去掉开头符号 ^ * _ 以及多余标点
            s = re.sub(r'^[\^*_\s]+', '', s)
            s = s.strip()
            # 避免产生空字符串
            return s

        # 作者候选区域
        author_segment = ''
        if m_pub:
            author_segment = entry[:m_pub.start()].strip()
        else:
            # 无出版日期：截取到第一个外部(非 wikipedia)链接或第一个引号包裹标题链接前
            first_ext_idx = None
            for t,u in content_links:
                if 'wikipedia.org' not in u:
                    # 位置基于全文搜索
                    pos = entry.find('[' + t + '](')
                    if pos != -1:
                        first_ext_idx = pos
                        break
            if first_ext_idx is not None:
                author_segment = entry[:first_ext_idx].strip()
            else:
                # 若没有外部链接，以第一个链接前为作者
                first_link_text = content_links[0][0]
                pos = entry.find('[' + first_link_text + '](')
                author_segment = entry[:pos].strip() if pos != -1 else ''

        # 清除作者段中的残留 jump/锚点链接
        if author_segment:
            author_segment = JUMP_CARET_PATTERN.sub(' ', author_segment)
            author_segment = JUMP_INLINE_ANCHORS_PATTERN.sub(' ', author_segment)
            author_segment = re.sub(r'(\[[^\]]+\]\(https?://[^)]+\))', lambda m: re.sub(r'^\[|\]\([^)]*\)$','',m.group(0)), author_segment)
            author_segment = re.sub(r'\[[^\]]+\]\(https?://[^)]+\)', lambda m: re.sub(r'^\[|\]\([^)]*\)$','', m.group(0)), author_segment)
            author_segment = re.sub(r'\s+', ' ', author_segment).strip()
            # 去掉末尾句点
            if author_segment.endswith('.'):
                author_segment = author_segment[:-1].strip()

        author = author_segment
        # 若作者过长 (> 15 词) 视为噪声放弃
        if author and len(author.split()) > 15:
            author = ''

        # 确定标题链接：优先策略
        title_link = None
        # 1. 在出版日期之后的链接中，优先选择文本被引号包裹且为外部链接
        after_pub_pos = m_pub.end() if m_pub else (len(author_segment) if author_segment else 0)
        for t,u in content_links:
            pos = entry.find('['+t+'](')
            if pos < after_pub_pos:
                continue
            txt = t.strip()
            if ('wikipedia.org' not in u) and re.match(r'^".*"$|^“.*”$', txt):
                title_link = (t,u)
                break
        # 2. 外部链接（日期之后）
        if not title_link:
            for t,u in content_links:
                pos = entry.find('['+t+'](')
                if pos < after_pub_pos:
                    continue
                if 'wikipedia.org' not in u:
                    title_link = (t,u)
                    break
        # 3. 任何（日期之后）
        if not title_link:
            for t,u in content_links:
                pos = entry.find('['+t+'](')
                if pos >= after_pub_pos:
                    title_link = (t,u)
                    break
        # 4. 回退：第一个不在作者段中的链接
        if not title_link:
            for t,u in content_links:
                pos = entry.find('['+t+'](')
                if not author_segment or pos >= len(author_segment):
                    title_link = (t,u)
                    break
        if not title_link:
            continue
        title_text, title_url = title_link

        # 查找 Archived 链接（记录但不用作主 url）
        archive_url = None
        for t,u in content_links:
            if t.lower() == 'archived':
                archive_url = u
                break

    # 提取 source：标题链接之后第一个非 Archived、非同 URL 的链接文本
        title_pos = entry.find('['+title_text+'](')
        source = ''
        for t,u in content_links:
            if t == title_text and u == title_url:
                continue
            pos = entry.find('['+t+'](')
            if pos < title_pos:
                continue
            if t.lower() == 'archived':
                continue
            if t.strip() == title_text.strip():
                continue
            source = t.strip('_* ')
            break

        # 若未找到来源或来源与作者相同，尝试解析标题后纯文本媒体名称（支持 NPR. 或 _The New Yorker_ 形式）
        if not source or source == author:
            link_markdown = f'[{title_text}]({title_url})'
            link_end = entry.find(link_markdown)
            if link_end != -1:
                link_end += len(link_markdown)
                tail = entry[link_end:]
                # 截断到 Archived / Retrieved 之前
                cut_idx = len(tail)
                for kw in ['Archived', 'Retrieved']:
                    kpos = tail.find(kw)
                    if kpos != -1 and kpos < cut_idx:
                        cut_idx = kpos
                tail_section = tail[:cut_idx]
                # 斜体媒体 _..._
                m_italic = re.search(r'_(\s*[^_]{2,}?)_', tail_section)
                candidate = ''
                if m_italic:
                    candidate = m_italic.group(1).strip()
                else:
                    # 首个以句点结束的连续大写/首字母大写词组 (NPR. / Associated Press.)
                    m_acro = re.match(r'\s*([A-Z][A-Za-z&\.]*?(?:\s+[A-Z][A-Za-z&\.]*?){0,4})\.(?:\s|$)', tail_section)
                    if m_acro:
                        cand = m_acro.group(1).strip()
                        # 过滤 Retrieved / Archived 误判
                        if cand.lower() not in {'retrieved', 'archived'}:
                            candidate = cand
                if not candidate:
                    # 捕获如 "NPR." 出现在标题链接后
                    m_npr = re.search(r'\b(NPR)\.(?:\s|$)', tail_section)
                    if m_npr:
                        candidate = m_npr.group(1)
                if candidate and candidate != author:
                    source = candidate.strip('_* ')

        # 过滤内部 works cited：若标题仍是 wikipedia 链接且没有外部链接
        if 'wikipedia.org' in title_url and not any('wikipedia.org' not in u for _,u in content_links):
            continue

        # 清洗标题
        title = title_text.strip().strip('"“”').strip('_* ')
        title = re.sub(r'\s+', ' ', title)

        # 作者/来源互补
        if not author and source:
            author = source
        if not source and author:
            source = author

        author = clean_name(author)
        source = clean_name(source)
        # 若清洗后仍为空且彼此存在互补
        if not author and source:
            author = source
        if not source and author:
            source = author

        is_external = ('wikipedia.org' not in title_url)
        item: Dict[str, Any] = {
            'title': title,
            'url': title_url,
            'is_external': is_external,
        }
        if author:
            item['author'] = author
        if source:
            item['source'] = source
        if archive_url:
            item['archive_url'] = archive_url
        if publish_date:
            item['publish_date'] = publish_date
        if retrieved_date:
            item['retrieved_date'] = retrieved_date
        items.append(item)
    return items


def write_jsonl(items: List[Dict[str, Any]], path: str) -> None:
    with open(path, 'w', encoding='utf-8') as f:
        for it in items:
            # 初始化 scraped 标记
            if 'scraped' not in it:
                it['scraped'] = False
            f.write(json.dumps(it, ensure_ascii=False) + '\n')


def main():
    parser = argparse.ArgumentParser(description='抓取网页并提取参考文献生成 JSONL')
    parser.add_argument('--url', required=True, help='目标网页 URL (基准 URL)')
    parser.add_argument('--output_dir', required=True, help='输出根目录')
    parser.add_argument('--api-key', dest='api_key', default=None, help='Jina API Key (可选)')
    args = parser.parse_args()

    api_key = args.api_key or os.environ.get('JINA_API_KEY') or DEFAULT_API_KEY
    if not api_key.startswith('Bearer '):
        api_key = f'Bearer {api_key}'

    scraper = WebScrapingJinaTool(api_key)
    data = scraper(args.url)
    md_path = save_markdown(data, args.output_dir)

    # 读取 markdown 内容
    with open(md_path, 'r', encoding='utf-8') as f:
        markdown_text = f.read()

    reference_dir = os.path.join(os.path.dirname(md_path), 'reference')
    os.makedirs(reference_dir, exist_ok=True)
    jsonl_path = os.path.join(reference_dir, 'references.jsonl')

    if os.path.exists(jsonl_path):
        print(f'References 已存在，跳过重建: {jsonl_path}')
    else:
        ref_lines = parse_references_block(markdown_text)
        ref_entries = group_reference_entries(ref_lines)
        items = build_reference_items(ref_entries)
        # 去重：完全相同的条目只保留一份
        from json import dumps
        seen = set()
        deduped = []
        for it in items:
            key = dumps(it, sort_keys=True, ensure_ascii=False)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(it)
        removed = len(items) - len(deduped)
        if removed > 0:
            print(f'去重: 移除 {removed} 条重复引用 (原始 {len(items)} -> 保留 {len(deduped)})')
        items = deduped
        write_jsonl(items, jsonl_path)
        print(f'Markdown: {md_path}')
        print(f'References extracted (after dedup): {len(items)} -> {jsonl_path}')
        if not items:
            print('警告: 未能解析到引用，可检查页面是否包含 References 段或改进解析逻辑。')


if __name__ == '__main__':
    main()
