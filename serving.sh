#!/bin/bash

# 多GPU OSS服务启动脚本
# 一次性启动8个服务：端口30061~30068，每个服务使用1张GPU卡

# 如果你是用镜像代理拉的，把 IMG 换成你本地的那个名字即可
# IMG=docker.m.daocloud.io/lmsysorg/sglang:v0.5.0rc2-cu126

IMG=hub-cn-shanghai-2.kce.ksyun.com/ystrain/ystrain:sglang.v0.5.0rc2-cu126.oss
CONTAINER_NAME=inference_oss_multi

# 配置参数
START_PORT=30061
END_PORT=30068
MODEL_PATH="/mnt/raid/model/gpt-oss-120b"
LOG_DIR="/mnt/jfs/xubenfeng/model/logs"
TIKTOKEN_CACHE="/mnt/jfs/xubenfeng/model/openai_harmony_vocab"

# 创建日志目录
echo "正在创建日志目录: $LOG_DIR"
mkdir -p $LOG_DIR

echo "正在检查是否存在同名容器..."
# 停止并删除已存在的同名容器
if docker ps -a --format 'table {{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "发现已存在容器 ${CONTAINER_NAME}，正在停止并删除..."
    docker stop ${CONTAINER_NAME} > /dev/null 2>&1
    docker rm ${CONTAINER_NAME} > /dev/null 2>&1
fi

echo "🚀 将启动 $((END_PORT - START_PORT + 1)) 个OSS服务"
echo "📊 端口范围: $START_PORT - $END_PORT"
echo "🎯 每个服务使用1张GPU卡"
echo "📝 日志目录: $LOG_DIR"

echo "正在启动容器 ${CONTAINER_NAME}..."
# 1) 起一个常驻的容器（用 sleep 保活），名字不要用斜杠
docker run -d --gpus all --rm -ti \
  --name ${CONTAINER_NAME} \
  --privileged --cap-add=IPC_LOCK \
  --ulimit memlock=-1 --ulimit stack=67108864 \
  --net=host --ipc=host \
  -v /mnt:/mnt \
  $IMG bash -lc "sleep infinity"

# 检查容器是否启动成功
if [ $? -ne 0 ]; then
    echo "错误：容器启动失败"
    exit 1
fi

echo "容器启动成功，等待3秒..."
sleep 3

echo "正在容器内启动多个 sglang 服务器..."

# 循环启动多个服务
for port in $(seq $START_PORT $END_PORT); do
    gpu_id=$((port - START_PORT))  # GPU ID: 0,1,2,3,4,5,6,7
    log_file="$LOG_DIR/server_port_${port}_gpu_${gpu_id}.log"
    
    echo "🔄 启动服务 GPU:$gpu_id Port:$port"
    
    # 在容器内启动单个服务
    docker exec ${CONTAINER_NAME} bash -c "
    export TIKTOKEN_RS_CACHE_DIR=$TIKTOKEN_CACHE
    export CUDA_VISIBLE_DEVICES=$gpu_id
    echo '正在启动 sglang 服务器 - GPU:$gpu_id Port:$port'
    nohup python3 -m sglang.launch_server \
      --model $MODEL_PATH \
      --tp 1 \
      --host 0.0.0.0 --port $port \
      --mem-fraction-static 0.9 > $log_file 2>&1 &
    echo 'GPU:$gpu_id Port:$port 服务已在后台启动'
    echo '日志文件: $log_file'
    "
    
    # 每个服务启动间隔2秒，避免同时启动造成资源竞争
    sleep 2
done

# 等待所有服务完全启动
echo "⏰ 等待60秒让所有服务完全启动..."
sleep 60

# 检查服务状态
echo "🔍 检查服务状态..."
running_services=0
for port in $(seq $START_PORT $END_PORT); do
    # 简单检查端口是否被监听
    if docker exec ${CONTAINER_NAME} bash -c "netstat -tuln | grep :$port" >/dev/null 2>&1; then
        echo "✅ Port $port: 运行中"
        running_services=$((running_services + 1))
    else
        echo "❌ Port $port: 未启动"
    fi
done

echo ""
echo "🎉 部署完成！"
echo "===================="
echo "容器名称: ${CONTAINER_NAME}"
echo "服务数量: $running_services/$((END_PORT - START_PORT + 1))"
echo "端口范围: $START_PORT - $END_PORT"
echo "日志目录: $LOG_DIR"
echo ""
echo "📝 常用命令:"
echo "查看所有日志: ls -la $LOG_DIR/"
echo "查看特定日志: tail -f $LOG_DIR/server_port_30061_gpu_0.log"
echo "进入容器: docker exec -it ${CONTAINER_NAME} bash"
echo "停止容器: docker stop ${CONTAINER_NAME}"
echo ""
echo "🌐 服务地址:"
for port in $(seq $START_PORT $END_PORT); do
    gpu_id=$((port - START_PORT))
    echo "  GPU$gpu_id: http://0.0.0.0:$port"
done
echo ""
echo "🧪 测试命令:"
echo "curl -X POST http://0.0.0.0:30061/v1/chat/completions \\"
echo "  -H \"Content-Type: application/json\" \\"
echo "  -d '{\"model\":\"default\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"}]}'"