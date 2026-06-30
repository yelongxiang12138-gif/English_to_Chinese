"""
Vercel Serverless 入口
将 Flask WSGI 应用暴露为 Vercel Serverless Function
"""
import sys
import os

# 将项目根目录加入 Python 路径，确保能 import backend 模块
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.app import app

# Vercel 需要这个变量名来识别 Flask 应用
