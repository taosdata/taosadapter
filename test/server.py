#!/usr/bin/env python3
from flask import Flask, request, jsonify
import subprocess
import os
import signal
import time

app = Flask(__name__)
taosadapter_process = None  # 存储子进程对象

@app.route('/start', methods=['POST'])
def start_taosadapter():
    global taosadapter_process
    if taosadapter_process is not None:
        return jsonify({"status": "error", "message": "taosadapter is already running"}), 400

    try:
        # 启动 taosadapter（假设在当前目录）
        taosadapter_process = subprocess.Popen(
            ["./taosadapter"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid  # 防止子进程被终端信号影响
        )
        time.sleep(5)  # 等待进程启动
        if taosadapter_process.poll() is not None:
            error = taosadapter_process.stderr.read().decode('utf-8')
            return jsonify({"status": "error", "message": f"Failed to start: {error}"}), 500

        return jsonify({"status": "success", "pid": taosadapter_process.pid}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/stop', methods=['POST'])
def stop_taosadapter():
    global taosadapter_process
    if taosadapter_process is None:
        return jsonify({"status": "error", "message": "taosadapter is not running"}), 400

    try:
        # 发送 SIGTERM 信号（优雅关闭）
        os.killpg(os.getpgid(taosadapter_process.pid), signal.SIGTERM)
        time.sleep(5)
        taosadapter_process = None
        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/status', methods=['GET'])
def status():
    global taosadapter_process
    if taosadapter_process is None or taosadapter_process.poll() is not None:
        return jsonify({"status": "stopped"}), 200
    else:
        return jsonify({"status": "running", "pid": taosadapter_process.pid}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
