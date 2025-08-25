#!/bin/bash

# ==================== 多GPU Qwen Embedding服务部署脚本 (原生Python) ====================

MODEL_PATH="/mnt/jfs/xubenfeng/rag/models_and_datasets/Qwen3-Embedding-0.6B"
START_PORT=30151
END_PORT=30158
LOG_DIR="/mnt/jfs/xubenfeng/model/logs/embedding"
SCRIPT_PATH="/mnt/jfs/xubenfeng/rag/reproduce_local/embedding_server.py"

echo "🚀 启动多GPU Qwen Embedding服务 (原生Python)..."
echo "🤖 模型路径: $MODEL_PATH"
echo "🔗 端口范围: $START_PORT-$END_PORT"
echo "📋 日志目录: $LOG_DIR"
echo "📄 服务脚本: $SCRIPT_PATH"

# 创建日志目录
mkdir -p $LOG_DIR

# 检查必要文件
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "❌ 错误: Embedding服务脚本不存在: $SCRIPT_PATH"
    exit 1
fi

if [ ! -d "$MODEL_PATH" ]; then
    echo "❌ 错误: 模型路径不存在: $MODEL_PATH"
    exit 1
fi

# 停止现有服务
echo "🛑 停止现有Embedding服务..."
pkill -f "embedding_server.py" || true
sleep 2

# 启动多个Embedding服务
echo "🚀 启动多个Embedding服务..."
for port in $(seq $START_PORT $END_PORT); do
    gpu_id=$((port - START_PORT))
    echo "🔧 启动服务: GPU $gpu_id, 端口 $port"
    
    # 启动Python服务
    CUDA_VISIBLE_DEVICES=$gpu_id python $SCRIPT_PATH \
        --model-path $MODEL_PATH \
        --host 0.0.0.0 \
        --port $port \
        --device cuda \
        > $LOG_DIR/embedding_port_${port}_gpu_${gpu_id}.log 2>&1 &
    
    # 间隔启动避免资源冲突
    sleep 2
done

# 等待服务启动
echo "⏳ 等待所有服务启动 (60秒)..."
sleep 60

# 检查服务状态
echo "📊 检查服务状态:"
active_services=0
for port in $(seq $START_PORT $END_PORT); do
    if netstat -tuln | grep -q ":$port "; then
        echo "✅ 端口 $port: 服务正常"
        active_services=$((active_services + 1))
    else
        echo "❌ 端口 $port: 服务异常"
    fi
done

echo ""
echo "📈 服务统计:"
echo "  - 启动成功: $active_services/8 个服务"
echo "  - 端口范围: $START_PORT-$END_PORT"
echo "  - GPU分配: 每服务1张GPU"
echo "  - 日志目录: $LOG_DIR"

# 测试第一个服务
echo ""
echo "🧪 测试服务连接:"
if curl -s -m 5 http://localhost:$START_PORT/health >/dev/null; then
    echo "✅ 服务连接测试成功"
else
    echo "❌ 服务连接测试失败"
fi

echo ""
echo "📋 管理命令:"
echo "  查看服务进程: ps aux | grep embedding_server.py"
echo "  查看端口占用: netstat -tuln | grep 3015"
echo "  查看日志: tail -f $LOG_DIR/embedding_port_30151_gpu_0.log"
echo "  停止所有服务: pkill -f embedding_server.py"
echo "  测试API: curl http://localhost:30151/health"

echo ""
echo "✅ 多GPU Embedding服务部署完成!"
