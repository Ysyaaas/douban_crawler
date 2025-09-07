#!/bin/bash

# 安装Redis
sudo apt-get update
sudo apt-get install -y redis-server

# 配置Redis允许外部访问
echo "Configuring Redis for external access..."
sudo sed -i 's/^bind 127.0.0.1 ::1/bind 0.0.0.0/' /etc/redis/redis.conf
sudo sed -i 's/^protected-mode yes/protected-mode no/' /etc/redis/redis.conf

# 重启Redis服务
sudo systemctl restart redis-server

# 检查服务状态
echo "Redis status:"
sudo systemctl status redis-server --no-pager

# 测试连接
echo -e "\nTesting connection:"
redis-cli -h 127.0.0.1 ping

echo -e "\nRedis is ready! Bind address: 0.0.0.0"