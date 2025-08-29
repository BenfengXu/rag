"""批量抓取引用来源页面

功能:
  读取 references.jsonl (由 extract_references.py 生成)，按指定行号范围抓取其中的 url，
  利用 jina_scraping.WebScrapingJinaTool 获取页面内容并保存为 Markdown。Markdown 文件标题直接
  使用 JSONL 中的 title，而不使用抓取结果返回的标题。支持断点/范围抓取。

使用示例:
  python fetch_reference_pages.py \
    --references /path/to/wiki_data/Joe Biden/reference/references.jsonl \
    --output-dir /path/to/wiki_data/Joe Biden/reference/ref_pages \
      --start 1 --end 100

参数:
  --references   引用 JSONL 路径 (必选)
  --output-dir   输出目录 (必选)
  --start        起始行号(1-based, 默认1)
  --end          结束行号(1-based, 默认读到末尾)
  --api-key      可选 Jina API Key (覆盖环境变量 JINA_API_KEY)
  --skip-exists  若目标 markdown 已存在则跳过 (默认开启，可用 --no-skip-exists 关闭)

输出结构:
  output_dir/
      <slug>.md  (文件名基于 title slug)

Markdown 模板:
  ---
  title: <JSONL title>
  source_url: <url>
  reference_line: <原始行号>
  fetched_title: <抓取返回的真实页面标题>
  fetched_publish_time: <抓取返回publish_time>
  ---
  # <JSONL title>
  <content>

注意: 若抓取失败，文件仍会生成（front matter 中包含 error 字段, content 为空）。
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Dict, Any, List, Tuple

from jina_scraping import WebScrapingJinaTool, DEFAULT_API_KEY, slugify  # type: ignore
from goliath import build_default_goliath_tool, GoliathTool  # type: ignore


def save_markdown(reference: Dict[str, Any], fetched: Dict[str, Any], output_dir: str, line_no: int, skip_exists: bool) -> str:
    """保存引用抓取结果为纯净 Markdown。

    变更：移除此前的 YAML front matter 与元数据行，只保留：
      # <title>
      <content>
    若抓取出现错误，文件将包含仅有标题与错误提示注释。
    """
    os.makedirs(output_dir, exist_ok=True)
    title = reference.get('title') or fetched.get('title') or 'Untitled'
    slug = slugify(title)
    filename = f"{slug}.md"
    path = os.path.join(output_dir, filename)

    if skip_exists and os.path.exists(path):
        return path

    parts: List[str] = []
    parts.append(f"# {title}\n")

    if 'error' in fetched:
        parts.append(f"<!-- fetch error: {fetched['error']} -->\n")
    else:
        content = fetched.get('content', '').rstrip() + '\n'
        parts.append(content)

    # 不再附加任何 meta 注释，保持纯文本内容

    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(parts))
    return path


def load_references(jsonl_path: str) -> List[Dict[str, Any]]:
    refs: List[Dict[str, Any]] = []
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line_stripped = line.strip()
            if not line_stripped:
                continue
            try:
                data = json.loads(line_stripped)
            except json.JSONDecodeError:
                data = {'error': 'json_decode_error', 'raw': line_stripped}
            refs.append(data)
    return refs


def save_references(jsonl_path: str, refs: List[Dict[str, Any]]):
    tmp_path = jsonl_path + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        for r in refs:
            f.write(json.dumps(r, ensure_ascii=False) + '\n')
    os.replace(tmp_path, jsonl_path)


def is_low_quality(content: str) -> Tuple[bool, str]:
    """判断内容是否为需要过滤的低质量页面。

    规则:
      1. 包含已知 404 / 错误页特征字符串之一
      2. 去除空行后行数 < 10
    返回 (需要过滤, 原因)
    """
    if not content:
        return True, 'empty_content'
    patterns = [
        '404 | Fox News',
        'It seems you clicked on a bad link',
        'Something has gone wrong',
        '404 Not Found',
        'Page Not Found'
    ]
    for p in patterns:
        if p in content:
            return True, f'match_pattern:{p}'
    lines = [l for l in content.splitlines() if l.strip()]
    if len(lines) < 10:
        return True, f'line_count<{10}'
    return False, ''


def main():
    parser = argparse.ArgumentParser(description='批量抓取 references.jsonl 中的 URL 内容并保存为 Markdown')
    parser.add_argument('--references', required=True, help='references.jsonl 路径')
    parser.add_argument('--output-dir', required=True, help='输出目录，用于存放抓取的引用页面 Markdown')
    parser.add_argument('--start', type=int, default=1, help='起始行号(1-based)')
    parser.add_argument('--end', type=int, default=None, help='结束行号(1-based, 包含)')
    parser.add_argument('--api-key', dest='api_key', default=None, help='Jina API Key (仅 fetcher=jina 时使用)')
    parser.add_argument('--fetcher', choices=['jina','goliath'], default='jina', help='[已弱化] 保留参数但逻辑已统一: 有 archive_url 时: goliath(archive)->jina(url)->goliath(url); 否则: jina(url)->goliath(url)')
    parser.add_argument('--skip-exists', dest='skip_exists', action='store_true', default=True, help='已存在文件则跳过 (默认)')
    parser.add_argument('--no-skip-exists', dest='skip_exists', action='store_false', help='即使存在也覆盖')
    parser.add_argument('--verbose', action='store_true', help='输出详细抓取尝试日志')
    parser.add_argument('--force', action='store_true', help='忽略 scraped 标记，强制重新抓取并覆盖 fetcher_used')
    parser.add_argument('--record-attempts', action='store_true', help='在引用条目中写入 attempt_log 详细尝试结果')
    args = parser.parse_args()

    fetcher_name = args.fetcher
    # 准备工具
    api_key = args.api_key or os.environ.get('JINA_API_KEY') or DEFAULT_API_KEY
    if not api_key.startswith('Bearer '):
        api_key = f'Bearer {api_key}'
    jina_tool = WebScrapingJinaTool(api_key)

    g_tool = None
    if fetcher_name in ('goliath','jina'):
        g_tool = build_default_goliath_tool()

    def normalize(data: dict) -> dict:
        return {
            'title': data.get('title',''),
            'content': data.get('content',''),
            'publish_time': data.get('publish_time','') or data.get('fetched_publish_time','')
        }

    def try_jina(url: str):
        d = jina_tool(url)
        if 'error' in d:
            return None, d.get('error','jina_error')
        return normalize(d), None

    def try_goliath(url: str):
        if g_tool is None:
            return None, 'goliath_not_initialized'
        d = g_tool(url)
        if not d.get('success') or d.get('error'):
            return None, d.get('error','goliath_error')
        return normalize(d), None

    def fetch_with_order(ref: dict):
        """按新统一策略尝试抓取。

        规则:
          - 若存在 archive_url: 依次尝试
              1) goliath(archive_url)
              2) jina(original url)
              3) goliath(original url)
          - 若不存在 archive_url:
              1) jina(original url)
              2) goliath(original url)
        返回: (norm, used_label, error) 其中 error 为 None 表示成功。
        """
        url_main = ref.get('url')
        archive_url = ref.get('archive_url')
        attempts = []  # (label, engine, url, success, error)
        sequence = []
        if archive_url:
            # 新顺序: 1) jina archive 2) goliath archive 3) jina main 4) goliath main
            sequence.append(('jina_archive', 'jina', archive_url))
            sequence.append(('goliath_archive', 'goliath', archive_url))
            sequence.append(('jina_main', 'jina', url_main))
            sequence.append(('goliath_main', 'goliath', url_main))
        else:
            sequence.append(('jina_main', 'jina', url_main))
            sequence.append(('goliath_main', 'goliath', url_main))

        for step_label, engine, url in sequence:
            if args.verbose:
                print(f"    -> 尝试 {step_label} ({engine}) url={url}")
            if engine == 'jina':
                norm, err = try_jina(url)
            else:
                norm, err = try_goliath(url)
            if err is None:
                # 标准化 used 标识
                if step_label == 'goliath_archive':
                    used = 'goliath(archive)'
                elif step_label == 'jina_archive':
                    used = 'jina(archive)'
                elif step_label == 'jina_main':
                    used = 'jina'
                elif step_label == 'goliath_main':
                    used = 'goliath'
                else:
                    used = step_label
                if args.verbose:
                    print(f"       成功 used={used} content_len={len(norm.get('content',''))}")
                attempts.append((step_label, engine, url, True, ''))
                return norm, used, attempts
            attempts.append((step_label, engine, url, False, err))
            if args.verbose or engine == 'goliath':
                print(f"       失败({engine}:{step_label}): {err}")

        # 全部失败
        err_msg = '; '.join(f"{lab}:{e}" for lab, _, _, _, e in attempts)
        return None, 'failed', attempts

    refs = load_references(args.references)
    total = 0
    success = 0
    failed = 0
    start = args.start if args.start >= 1 else 1
    end = args.end if args.end is not None else len(refs)
    end = min(end, len(refs))

    for idx in range(start, end + 1):  # idx 是 1-based 行号
        ref = refs[idx - 1]
        total += 1
        url = ref.get('url')
        if not url:
            failed += 1
            print(f"[line {idx}] 缺少 url，跳过")
            continue
        if ref.get('scraped') is True and not args.force:
            print(f"[line {idx}] 已爬取，跳过 (可用 --force 重新抓取)")
            continue
        print(f"[line {idx}] 抓取 {url} ...")
        max_filter_retry = 3
        filter_attempts = 0
        aggregate_attempt_log = []  # 合并多轮尝试
        saved = False
        while True:
            norm, used, attempts_or_err = fetch_with_order(ref)
            # 失败整体（网络/工具均失败）
            if norm is None:
                failed += 1
                if args.record_attempts:
                    aggregate_attempt_log.extend([
                        {
                            'round': filter_attempts + 1,
                            'step': a[0],
                            'engine': a[1],
                            'url': a[2],
                            'success': a[3],
                            'error': a[4]
                        } for a in attempts_or_err
                    ])
                    ref['attempt_log'] = aggregate_attempt_log
                err_msg = '; '.join(f"{a[0]}:{a[4]}" for a in attempts_or_err if a[4]) or 'unknown_error'
                print(f"  失败(fetcher_used={used}): {err_msg} (不保存文件)")
                break
            # 成功获取内容，检测低质量
            filt, reason = is_low_quality(norm.get('content',''))
            if args.record_attempts:
                aggregate_attempt_log.extend([
                    {
                        'round': filter_attempts + 1,
                        'step': a[0],
                        'engine': a[1],
                        'url': a[2],
                        'success': a[3],
                        'error': a[4]
                    } for a in attempts_or_err
                ])
            if filt:
                filter_attempts += 1
                print(f"  低质量内容(原因={reason}) 第 {filter_attempts} 次")
                if filter_attempts < max_filter_retry:
                    print("  -> 重试抓取...")
                    continue
                else:
                    # 达到最大重试，标记为已处理但不保存文件
                    ref['scraped'] = True
                    ref['fetcher_used'] = used + '(low_quality)' if used else 'low_quality'
                    ref['filter_reason'] = reason
                    if args.record_attempts:
                        ref['attempt_log'] = aggregate_attempt_log
                    success += 1  # 视为处理完毕
                    print(f"  三次均低质量(原因={reason}) 标记 scraped=true 不保存文件")
                    break
            else:
                # 内容可接受，保存
                success += 1
                ref['scraped'] = True
                ref['fetcher_used'] = used
                if args.record_attempts:
                    ref['attempt_log'] = aggregate_attempt_log
                out_path = save_markdown(ref, norm, args.output_dir, idx, args.skip_exists)
                print(f"  保存 -> {out_path} (fetcher_used={used})")
                saved = True
                break
        # 实时写回（可选：为减少 IO 可每 N 次写回）
        save_references(args.references, refs)

    print(f"完成: total={total} success={success} failed={failed}")
    # 统计总结（基于 refs 中记录的 fetcher_used）
    from collections import Counter
    used_counter = Counter(r.get('fetcher_used') for r in refs if r.get('scraped'))
    if used_counter:
        print("成功来源统计:")
        for k,v in used_counter.items():
            print(f"  {k}: {v}")


if __name__ == '__main__':
    main()
