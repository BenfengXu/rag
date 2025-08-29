#!/usr/bin/env bash
set -euo pipefail

# 用法:
#   bash run_ref_scraper.sh <wiki_url> [start_line] [end_line] [--fetcher jina|goliath]
#   bash run_ref_scraper.sh <wiki_url> --all [--fetcher jina|goliath]   # 抓取全部引用
#   bash run_ref_scraper.sh --csv <csv_file> [start_row] [end_row] [--fetcher jina|goliath]  # CSV批量处理
#   bash run_ref_scraper.sh --csv <csv_file> --all [--fetcher jina|goliath]   # CSV全部处理
# 说明:
#   默认策略 (已统一，不再强依赖 --fetcher):
#     若引用项含 archive_url:  goliath(archive_url) -> jina(原始url) -> goliath(原始url)
#     若无 archive_url:        jina(原始url) -> goliath(原始url)
#   --fetcher 参数保留仅作显示，内部逻辑以以上顺序为准。
# 示例:
#   bash run_ref_scraper.sh https://en.wikipedia.org/wiki/Joe_Biden 1 50
#   bash run_ref_scraper.sh https://en.wikipedia.org/wiki/Joe_Biden --all
#   bash run_ref_scraper.sh --csv high_quality_wikipages.csv 2 11
#   bash run_ref_scraper.sh --csv high_quality_wikipages.csv --all

# 参数解析
CSV_MODE=0
CSV_FILE=""
URL=""
ARG2=""
ARG3=""
ARG4=""
FETCHER="jina"  # 保留参数; 实际抓取顺序见顶部注释

# 检查第一个参数是否为 --csv
if [[ "${1:-}" == "--csv" ]]; then
    CSV_MODE=1
    CSV_FILE="${2:-}"
    ARG2="${3:-}"
    ARG3="${4:-}"
    ARG4="${5:-}"
    
    if [[ -z "$CSV_FILE" ]]; then
        echo "[ERROR] --csv 模式需要指定CSV文件路径" >&2
        echo "用法: bash $0 --csv <csv_file> [start_row] [end_row] [--fetcher jina|goliath]" >&2
        exit 1
    fi
    
    if [[ ! -f "$CSV_FILE" ]]; then
        echo "[ERROR] CSV文件不存在: $CSV_FILE" >&2
        exit 1
    fi
else
    # 原单个URL模式
    URL="${1:-https://en.wikipedia.org/wiki/Beyonc%C3%A9}"
    ARG2="${2:-}"
    ARG3="${3:-}"
    ARG4="${4:-}"
fi

# 支持 --fetcher 参数在第3或第4位置
for arg in "$ARG2" "$ARG3" "$ARG4"; do
    if [[ "$arg" == --fetcher* ]]; then
        # 形式 --fetcher=goliath 或 --fetcher goliath
        if [[ "$arg" == *=* ]]; then
            FETCHER="${arg#*=}"
        else
            # 下一个参数取值
            : # 已简化；调用者用 = 形式即可
        fi
    fi
done
if [[ "$FETCHER" != "jina" && "$FETCHER" != "goliath" ]]; then
    echo "[WARN] 未知 fetcher=$FETCHER, 回退 jina" >&2
    FETCHER="jina"
fi

ROOT_DIR="/mnt/jfs/wangpengyu/UltraWikiDomain/raw"

# CSV模式处理
if [[ $CSV_MODE -eq 1 ]]; then
    echo "[INFO] CSV模式: 从文件 $CSV_FILE 读取URLs"
    
    # 检查是否为 --all 模式
    ALL_MODE=0
    if [[ "$ARG2" == "--all" ]]; then
        ALL_MODE=1
        START=2  # CSV第一行是标题，从第2行开始
        END=$(wc -l < "$CSV_FILE" | tr -d ' ')
    else
        START=${ARG2:-2}  # 默认从第2行开始（跳过标题）
        END=${ARG3:-11}   # 默认处理10行
    fi
    
    echo "[INFO] 处理CSV行范围: $START .. $END"
    
    # 读取CSV文件并处理每个URL
    line_num=1
    while IFS=',' read -r title url || [[ -n "$title" ]]; do
        # 跳过标题行
        if [[ $line_num -eq 1 ]]; then
            line_num=$((line_num + 1))
            continue
        fi
        
        # 检查是否在处理范围内
        if [[ $line_num -lt $START ]]; then
            line_num=$((line_num + 1))
            continue
        fi
        
        if [[ $line_num -gt $END ]]; then
            break
        fi
        
        # 清理URL（去除可能的引号和空格）
        url=$(echo "$url" | sed 's/^[[:space:]]*"//;s/"[[:space:]]*$//' | tr -d '\r\n')
        title=$(echo "$title" | sed 's/^[[:space:]]*"//;s/"[[:space:]]*$//' | tr -d '\r\n')
        
        if [[ -z "$url" || "$url" == "url" ]]; then
            echo "[WARN] 第 $line_num 行: URL为空，跳过"
            line_num=$((line_num + 1))
            continue
        fi
        
        echo ""
        echo "==================== 处理第 $line_num 行 ===================="
        echo "[INFO] 标题: $title"
        echo "[INFO] URL: $url"
        
        # 使用当前URL替换全局URL变量
        CURRENT_URL="$url"
        
        # 处理当前URL（复制原有逻辑）
        echo "[INFO] 抓取 URL: $CURRENT_URL"
        echo "[INFO] 开始提取引用..."
        python tools/extract_references.py \
            --url "$CURRENT_URL" \
            --output_dir "$ROOT_DIR"
        
        # 从 URL 中提取 slug（最后一段）并转换为标题形式 (下划线 -> 空格)
        ARTICLE_SLUG=${CURRENT_URL##*/}
        ARTICLE_TITLE=${ARTICLE_SLUG//_/ }
        
        REFERENCE_DIR="$ROOT_DIR/$ARTICLE_TITLE/reference"
        REF_JSONL="$REFERENCE_DIR/references.jsonl"
        
        if [[ ! -f "$REF_JSONL" ]]; then
            echo "[WARN] 未找到 $REF_JSONL ，跳过此URL"
            line_num=$((line_num + 1))
            continue
        fi
        
        # 对于CSV模式，默认抓取前10个引用
        CSV_START=1
        CSV_END=10
        
        OUTPUT_PAGES_DIR="$REFERENCE_DIR/reference_pages"
        mkdir -p "$OUTPUT_PAGES_DIR"
        
        echo "[INFO] 引用文件: $REF_JSONL"
        echo "[INFO] 输出引用页面目录: $OUTPUT_PAGES_DIR"
        echo "[INFO] 引用抓取工具: $FETCHER"
        
        python tools/fetch_reference_pages.py \
            --references "$REF_JSONL" \
            --output-dir "$OUTPUT_PAGES_DIR" \
            --start "$CSV_START" --end "$CSV_END" \
            --fetcher "$FETCHER"
        
        line_num=$((line_num + 1))
        
        # 添加延迟避免过于频繁的请求
        sleep 2
    done < "$CSV_FILE"
    
    echo ""
    echo "[DONE] CSV批量处理完成。"
    exit 0
fi

# 原有的单URL处理逻辑保持不变
ALL_MODE=0
if [[ "$ARG2" == "--all" ]]; then
    ALL_MODE=1
    START=1  # 先占位，稍后根据行数设定 END
    END=1
else
    START=${ARG2:-1}
    END=${ARG3:-10}
fi

echo "[INFO] 抓取 URL: $URL"
if [[ $ALL_MODE -eq 1 ]]; then
    echo "[INFO] 模式: 抓取全部引用 (--all)"
else
    echo "[INFO] 目标范围: lines $START .. $END"
fi

# 先运行引用抽取，生成 reference/references.jsonl
python tools/extract_references.py \
    --url "$URL" \
    --output_dir "$ROOT_DIR"

# 从 URL 中提取 slug（最后一段）并转换为标题形式 (下划线 -> 空格)
ARTICLE_SLUG=${URL##*/}
ARTICLE_TITLE=${ARTICLE_SLUG//_/ }

REFERENCE_DIR="$ROOT_DIR/$ARTICLE_TITLE/reference"
REF_JSONL="$REFERENCE_DIR/references.jsonl"

if [[ ! -f "$REF_JSONL" ]]; then
    echo "[WARN] 未找到 $REF_JSONL ，尝试列出可能的目录匹配：" >&2
    echo "[INFO] 可用目录:"
    find "$ROOT_DIR" -maxdepth 1 -type d -printf '  %f\n'
    echo "[ERROR] 退出。请检查页面标题与文件夹名称是否一致。" >&2
    exit 1
fi

# 如果是 --all 模式，则统计引用总行数 (每行一个 JSON 对象)
if [[ $ALL_MODE -eq 1 ]]; then
    TOTAL_LINES=$(wc -l < "$REF_JSONL" | tr -d ' ' || true)
    if [[ -z "$TOTAL_LINES" || "$TOTAL_LINES" -le 0 ]]; then
        echo "[WARN] references.jsonl 为空，退出。"
        exit 0
    fi
    START=1
    END=$TOTAL_LINES
    echo "[INFO] references.jsonl 共 $TOTAL_LINES 行，全部抓取。"
fi

OUTPUT_PAGES_DIR="$REFERENCE_DIR/reference_pages"
mkdir -p "$OUTPUT_PAGES_DIR"

echo "[INFO] 引用文件: $REF_JSONL"
echo "[INFO] 输出引用页面目录: $OUTPUT_PAGES_DIR"
echo "[INFO] 引用抓取工具: $FETCHER"

python tools/fetch_reference_pages.py \
    --references "$REF_JSONL" \
    --output-dir "$OUTPUT_PAGES_DIR" \
    --start "$START" --end "$END" \
    --fetcher "$FETCHER"

echo "[DONE] 处理完成。"