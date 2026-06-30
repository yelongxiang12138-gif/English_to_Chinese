"""
全书翻译 - 后端代理服务
保护 API Key，转发请求到 Dify Workflow API
支持 streaming（优先）和 blocking（fallback）两种模式
"""
import json
import os
import time
from flask import Flask, request, Response, jsonify, stream_with_context, send_from_directory
from flask_cors import CORS
import requests

app = Flask(__name__, static_folder=None)
CORS(app)

# 前端目录：本地和 Vercel 环境自动适配
# 本地: backend/../frontend -> frontend/
# Vercel: backend/ 在项目根，frontend/ 同级
_backend_dir = os.path.dirname(os.path.abspath(__file__))
_frontend_candidates = [
    os.path.join(os.path.dirname(_backend_dir), 'frontend'),
    os.path.join(_backend_dir, '..', 'frontend'),
]
FRONTEND_DIR = next((d for d in _frontend_candidates if os.path.isdir(d)), _frontend_candidates[0])

DIFY_BASE_URL = "https://api.dify.ai/v1"
DIFY_API_KEY = os.environ.get("DIFY_API_KEY", "app-tG3yKAkF6m2WhHCgOdmfoH36")

# 请求超时时间（秒），根据文本长度动态调整
TIMEOUT_PER_CHAR = 2.0
MIN_TIMEOUT = 30
MAX_TIMEOUT = 600


def get_dify_headers():
    return {
        "Authorization": f"Bearer {DIFY_API_KEY}",
        "Content-Type": "application/json"
    }


def call_dify_streaming(body, headers, timeout):
    """尝试 streaming 模式"""
    return requests.post(
        f"{DIFY_BASE_URL}/workflows/run",
        json=body,
        headers=headers,
        stream=True,
        timeout=timeout
    )


def call_dify_blocking(body, headers, timeout):
    """使用 blocking 模式"""
    body_copy = dict(body)
    body_copy["response_mode"] = "blocking"
    return requests.post(
        f"{DIFY_BASE_URL}/workflows/run",
        json=body_copy,
        headers=headers,
        timeout=timeout
    )


def wrap_blocking_result_as_sse(data):
    """将 blocking 结果包装为 SSE 事件流"""
    outputs = data.get("data", {}).get("outputs", {})
    final_text = outputs.get("final", outputs.get("text", ""))

    # 模拟 workflow_finished 事件
    event = {
        "event": "workflow_finished",
        "workflow_run_id": data.get("workflow_run_id", ""),
        "task_id": data.get("task_id", ""),
        "data": {
            "status": "succeeded",
            "outputs": {"final": final_text},
            "elapsed_time": data.get("data", {}).get("elapsed_time", 0),
            "total_tokens": data.get("data", {}).get("total_tokens", 0)
        }
    }
    return f"data: {json.dumps(event, ensure_ascii=False)}\n\n"


@app.route('/api/translate', methods=['POST'])
def translate():
    """
    翻译接口 - 调用 Dify Workflow 进行全书翻译
    优先使用 streaming 模式，SSL失败时自动 fallback 到 blocking
    """
    data = request.get_json()
    input_text = data.get('text', '')
    user = data.get('user', 'book-translator-user')

    if not input_text.strip():
        return jsonify({"error": "请输入需要翻译的文本"}), 400

    headers = get_dify_headers()

    # 根据文本长度动态计算超时时间
    timeout = max(MIN_TIMEOUT, min(len(input_text) * TIMEOUT_PER_CHAR, MAX_TIMEOUT))

    body = {
        "inputs": {"input_text": input_text},
        "response_mode": "streaming",
        "user": user
    }

    # 尝试 streaming 模式
    try:
        resp = call_dify_streaming(body, headers, timeout)

        if resp.status_code == 200:
            # Streaming 成功，流式转发
            def generate():
                try:
                    for chunk in resp.iter_content(chunk_size=None, decode_unicode=True):
                        if chunk:
                            yield chunk
                except Exception:
                    # SSE 流中断，这是预期内的行为
                    pass

            return Response(
                stream_with_context(generate()),
                content_type='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache',
                    'X-Accel-Buffering': 'no'
                }
            )

        if resp.status_code != 200:
            error_text = resp.text[:500]
            # 也可能返回的不是 streaming，尝试作为 JSON 解析
            try:
                err_data = resp.json()
                error_text = err_data.get('message', error_text)
            except Exception:
                pass
            return jsonify({"error": f"翻译服务异常 ({resp.status_code}): {error_text}"}), resp.status_code

    except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
        # SSL/连接失败，fallback 到 blocking 模式
        print(f"[Fallback] Streaming SSL error, retrying with blocking mode: {e}")

    except Exception as e:
        # 其他错误也尝试 fallback
        print(f"[Fallback] Streaming error: {e}, retrying with blocking mode")

    # Fallback: blocking 模式
    try:
        resp = call_dify_blocking(body, headers, timeout)

        if resp.status_code == 200:
            result_data = resp.json()

            def generate_blocking():
                yield wrap_blocking_result_as_sse(result_data)

            return Response(
                stream_with_context(generate_blocking()),
                content_type='text/event-stream',
                headers={
                    'Cache-Control': 'no-cache',
                    'X-Accel-Buffering': 'no'
                }
            )

        return jsonify({"error": f"翻译服务异常 ({resp.status_code}): {resp.text[:500]}"}), resp.status_code

    except Exception as e:
        return jsonify({"error": f"翻译请求失败: {str(e)}"}), 500


@app.route('/api/health', methods=['GET'])
def health():
    """健康检查"""
    return jsonify({"status": "ok"})


@app.route('/')
def index():
    """提供前端首页"""
    return send_from_directory(FRONTEND_DIR, 'index.html')


@app.route('/<path:path>')
def static_files(path):
    """提供前端静态文件"""
    file_path = os.path.join(FRONTEND_DIR, path)
    if os.path.exists(file_path):
        return send_from_directory(FRONTEND_DIR, path)
    return send_from_directory(FRONTEND_DIR, 'index.html')


if __name__ == '__main__':
    print("Book Translator Backend running on http://localhost:5001")
    print(f"Frontend served from: {FRONTEND_DIR}")
    app.run(host='0.0.0.0', port=5001, debug=True)
