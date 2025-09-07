import hashlib
import os
import socket


def generate_fingerprint(url):
    """生成URL指纹（SHA1哈希）"""
    return hashlib.sha1(url.encode()).hexdigest()


def get_node_id():
    """获取节点ID（可通过环境变量覆盖）"""
    return os.getenv('NODE_ID', f"node_{socket.gethostname()}")