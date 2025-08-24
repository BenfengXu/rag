#!/bin/bash

# LightRAG本地模型实验运行脚本
# 使用OSS API + Qwen-Embedding-0.6B

echo "🚀 开始LightRAG本地模型实验"
echo "使用 OSS API (LLM) + Qwen-Embedding-0.6B"
echo "========================================"

# 检查数据集目录
DATASET_DIR="/mnt/jfs/xubenfeng/rag/models_and_datasets/UltraDomain"
if [ ! -d "$DATASET_DIR" ]; then
    echo "❌ 数据集目录不存在: $DATASET_DIR"
    echo "请确保UltraDomain数据集已下载"
    exit 1
fi

echo "✅ 数据集目录: $DATASET_DIR"

# ============== OSS 服务配置 ==============
# 设置OSS主机和端口
# 支持单端口: OSS_PORTS="30066"
# 支持多端口负载均衡: OSS_PORTS="30061,30062,30063,30064,30065,30066,30067,30068"
OSS_HOST="10.0.4.178"
OSS_PORTS="30061,30062,30063,30064,30065,30066,30067,30068"
OSS_PORTS="30066"

export OSS_HOST
export OSS_PORTS

echo "🌐 OSS服务配置:"
echo "  主机: $OSS_HOST"
if [[ "$OSS_PORTS" == *","* ]]; then
    port_count=$(echo "$OSS_PORTS" | tr ',' '\n' | wc -l)
    echo "  负载均衡: 启用 ($port_count 个端口)"
    echo "  端口列表: $OSS_PORTS"
else
    echo "  单一服务: $OSS_HOST:$OSS_PORTS"
fi

# ============== 实验参数配置 ==============
# 设置类别（可以修改）
CLASS="agriculture"
export CLASS
echo "📊 处理类别: $CLASS"

# 并发配置（根据OSS服务数量调整）
export MAX_ASYNC=50  # LLM并发
export EMBEDDING_FUNC_MAX_ASYNC=100  # Embedding并发
echo "⚡ 并发配置:"
echo "  MAX_ASYNC: $MAX_ASYNC"
echo "  EMBEDDING_FUNC_MAX_ASYNC: $EMBEDDING_FUNC_MAX_ASYNC"

# 创建必要的目录
echo "📁 创建目录结构..."
mkdir -p ../exp_results/data/unique_contexts
mkdir -p ../exp_results/data/questions
mkdir -p ../exp_results/data/results
mkdir -p ../exp_results/data/evaluations
mkdir -p ../exp_results/kg

# # >>>>>>>>>> Step 0: 数据预处理
# echo ""
# echo "🔄 Step 0: 数据预处理"
# echo "从UltraDomain提取唯一上下文..."
# python Step_0.py -i "$DATASET_DIR" -o ../exp_results/data/unique_contexts
# if [ $? -ne 0 ]; then
#     echo "❌ Step 0 失败"
#     exit 1
# fi
# echo "✅ Step 0 完成"

# 检查预处理结果
CONTEXT_FILE="../exp_results/data/unique_contexts/${CLASS}_unique_contexts.json"
if [ ! -f "$CONTEXT_FILE" ]; then
    echo "❌ 预处理文件不存在: $CONTEXT_FILE"
    exit 1
fi

# # >>>>>>>>>> Step 1: 构建知识图谱
# echo ""
# echo "🔄 Step 1: 构建知识图谱"
# echo "使用OSS API和Qwen Embedding..."
# python Step_1_local.py
# if [ $? -ne 0 ]; then
#     echo "❌ Step 1 失败"
#     exit 1
# fi
# echo "✅ Step 1 完成"

# # >>>>>>>>>> Step 2: 生成查询问题
# echo ""
# echo "🔄 Step 2: 生成查询问题"
# echo "使用OSS API生成高层次查询..."
# python Step_2_local.py
# if [ $? -ne 0 ]; then
#     echo "❌ Step 2 失败"
#     exit 1
# fi
# echo "✅ Step 2 完成"

# 检查问题文件
QUESTIONS_FILE="../exp_results/data/questions/${CLASS}_questions.txt"
if [ ! -f "$QUESTIONS_FILE" ]; then
    echo "❌ 问题文件不存在: $QUESTIONS_FILE"
    exit 1
fi

# >>>>>>>>>> Step 3: 执行查询
# echo ""
# echo "🔄 Step 3: 执行查询"
# echo "运行不同模式的查询..."
# python Step_3_local.py

# if [ $? -ne 0 ]; then
#     echo "❌ Step 3 失败"
#     exit 1
# fi
# echo "✅ Step 3 完成"

# 检查结果文件
NAIVE_RESULTS="../exp_results/data/results/${CLASS}_naive_results.json"
HYBRID_RESULTS="../exp_results/data/results/${CLASS}_hybrid_results.json"

if [ ! -f "$NAIVE_RESULTS" ] || [ ! -f "$HYBRID_RESULTS" ]; then
    echo "❌ 查询结果文件不完整"
    exit 1
fi

# >>>>>>>>>> Step 4: 批量评估 (可选)
echo ""
echo "🔄 Step 4: 批量评估 (naive vs hybrid)"
echo "使用OSS API评估查询结果..."
python batch_eval_local.py

if [ $? -ne 0 ]; then
    echo "⚠️  Step 4 失败，但实验主要部分已完成"
else
    echo "✅ Step 4 完成"
fi

echo ""
echo "🎉 实验完成！"
echo "========================================"
echo "📊 结果文件位置:"
echo "  - 知识图谱: ../exp_results/kg/${CLASS}/"
echo "  - 查询结果: ../exp_results/data/results/"
echo "  - 评估结果: ../exp_results/data/evaluations/"
echo ""
echo "📝 可以查看以下文件:"
echo "  - 问题: $QUESTIONS_FILE"
echo "  - Naive结果: $NAIVE_RESULTS"
echo "  - Hybrid结果: $HYBRID_RESULTS"
echo ""
echo "✨ 实验成功使用本地模型完成！"
