#!/bin/bash

# 在日志文件名中添加时间戳
LOG_FILE="benchmark_log_$(date +"%Y%m%d_%H%M%S").txt"

# 清空或创建日志文件
echo "=== Benchmark 运行日志 ===" > "$LOG_FILE"
echo "开始时间: $(date)" >> "$LOG_FILE"
echo "------------------------" >> "$LOG_FILE"

# 定义要执行的命令列表
COMMANDS=(
    "./taosadapter_bench -test.v -test.run TestTcp$"
    "./taosadapter_bench -test.v -test.run TestTcpConcurrent$"
    "./taosadapter_bench -test.v -test.run TestWs$"
    "./taosadapter_bench -test.v -test.run TestWsConcurrent$"
)

# 循环5次执行每个命令
for i in {1..5}
do
    echo "正在执行第 $i 次运行..."
    echo "[Run $i] 开始时间: $(date +"%Y-%m-%d %H:%M:%S")" >> "$LOG_FILE"

    for CMD in "${COMMANDS[@]}"
    do
        # 发送 start 请求
        echo "发送 start 请求..." >> "$LOG_FILE"
        curl -X POST http://192.168.1.43:5000/start >> "$LOG_FILE" 2>&1
        echo "start 请求已发送。" >> "$LOG_FILE"

        # 记录开始时间（秒级时间戳）
        START_TIME=$(date +%s)

        # 执行命令并记录输出
        echo "$(date +"%Y-%m-%d %H:%M:%S") running command: $CMD" >> "$LOG_FILE"
        $CMD | tee -a "$LOG_FILE"

        # 记录结束时间（秒级时间戳）
        END_TIME=$(date +%s)
        ELAPSED_TIME=$((END_TIME - START_TIME))

        # 发送 stop 请求
        echo "发送 stop 请求..." >> "$LOG_FILE"
        curl -X POST http://192.168.1.43:5000/stop >> "$LOG_FILE" 2>&1
        echo "stop 请求已发送。" >> "$LOG_FILE"

        echo "Command: $CMD" >> "$LOG_FILE"
        echo "[Run $i] 结束时间: $(date +"%Y-%m-%d %H:%M:%S")" >> "$LOG_FILE"
        echo "[Run $i] 总耗时: $ELAPSED_TIME 秒" >> "$LOG_FILE"
        echo "------------------------" >> "$LOG_FILE"

        echo "命令 $CMD 第 $i 次运行完成，耗时 $ELAPSED_TIME 秒"
    done
done

echo "所有5次运行已完成，日志已保存到 $LOG_FILE"