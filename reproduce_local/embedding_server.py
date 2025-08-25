#!/usr/bin/env python3
"""
Qwen Embedding HTTP服务器
支持OpenAI兼容的API格式
"""

import os
import sys
import argparse
import json
import time
from flask import Flask, request, jsonify
import torch
import numpy as np
from transformers import AutoTokenizer, AutoModel
from typing import List

class QwenEmbeddingServer:
    def __init__(self, model_path: str, device: str = None):
        self.model_path = model_path
        self.device = device if device else ("cuda" if torch.cuda.is_available() else "cpu")
        
        print(f"🤖 加载Qwen Embedding模型: {model_path}")
        print(f"📱 设备: {self.device}")
        
        # 加载模型
        self.tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)
        self.model = AutoModel.from_pretrained(model_path, trust_remote_code=True)
        self.model.eval()
        self.model = self.model.to(self.device)
        
        print(f"✅ 模型加载完成!")
        
    def encode_texts(self, texts: List[str]) -> np.ndarray:
        """编码文本为向量"""
        try:
            # Tokenize
            inputs = self.tokenizer(
                texts,
                padding=True,
                truncation=True,
                return_tensors="pt",
                max_length=512
            )
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            # 生成embeddings
            with torch.no_grad():
                outputs = self.model(**inputs)
                embeddings = outputs.last_hidden_state.mean(dim=1)
                embeddings = embeddings.cpu().numpy()
            
            return embeddings
            
        except Exception as e:
            print(f"❌ Embedding生成失败: {e}")
            raise e

def create_app(embedding_server: QwenEmbeddingServer):
    """创建Flask应用"""
    app = Flask(__name__)
    
    @app.route('/v1/embeddings', methods=['POST'])
    def get_embeddings():
        """OpenAI兼容的embeddings接口"""
        try:
            data = request.get_json()
            
            # 解析输入
            input_texts = data.get('input', [])
            if isinstance(input_texts, str):
                input_texts = [input_texts]
            
            model_name = data.get('model', 'qwen-embedding')
            
            # 生成embeddings
            start_time = time.time()
            embeddings = embedding_server.encode_texts(input_texts)
            end_time = time.time()
            
            # 构造响应
            response = {
                "object": "list",
                "data": [],
                "model": model_name,
                "usage": {
                    "prompt_tokens": sum(len(text.split()) for text in input_texts),
                    "total_tokens": sum(len(text.split()) for text in input_texts)
                }
            }
            
            for i, embedding in enumerate(embeddings):
                response["data"].append({
                    "object": "embedding",
                    "index": i,
                    "embedding": embedding.tolist()
                })
            
            print(f"✅ 处理 {len(input_texts)} 个文本, 用时: {end_time-start_time:.3f}s")
            return jsonify(response)
            
        except Exception as e:
            print(f"❌ 请求处理失败: {e}")
            return jsonify({"error": str(e)}), 500
    
    @app.route('/v1/models', methods=['GET'])
    def list_models():
        """列出可用模型"""
        return jsonify({
            "object": "list",
            "data": [{
                "id": "qwen-embedding",
                "object": "model",
                "created": int(time.time()),
                "owned_by": "local"
            }]
        })
    
    @app.route('/health', methods=['GET'])
    def health_check():
        """健康检查"""
        return jsonify({"status": "healthy"})
    
    return app

def main():
    parser = argparse.ArgumentParser(description="Qwen Embedding HTTP服务器")
    parser.add_argument("--model-path", required=True, help="模型路径")
    parser.add_argument("--host", default="0.0.0.0", help="服务地址")
    parser.add_argument("--port", type=int, required=True, help="端口号")
    parser.add_argument("--device", default=None, help="设备 (cuda/cpu)")
    
    args = parser.parse_args()
    
    # 创建Embedding服务器
    embedding_server = QwenEmbeddingServer(
        model_path=args.model_path,
        device=args.device
    )
    
    # 创建Flask应用
    app = create_app(embedding_server)
    
    print(f"🚀 启动Qwen Embedding服务:")
    print(f"  地址: http://{args.host}:{args.port}")
    print(f"  模型: {args.model_path}")
    print(f"  设备: {embedding_server.device}")
    
    # 启动服务
    app.run(host=args.host, port=args.port, threaded=True)

if __name__ == "__main__":
    main()
