from flask import Blueprint

# 创建蓝图对象
api = Blueprint("api_1_0", __name__)

from . import index,initialize_serializer,test_serializer,task
