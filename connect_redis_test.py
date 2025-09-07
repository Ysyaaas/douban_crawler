import redis

def test_redis_connection(host: str, port=6379):
    r = redis.Redis(host=host, port=port)
    try:
        response = r.ping()
        print("Redis连接成功!" if response else "连接失败")
    except redis.ConnectionError:
        print("无法连接到Redis服务器")

if __name__ == '__main__':
    host = "10.109.253.108"  # Redis服务器节点IP
    test_redis_connection(host)