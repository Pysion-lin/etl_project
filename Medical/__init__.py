from flask import Flask,g
from flask_sqlalchemy import SQLAlchemy
from flask_wtf.csrf import CSRFProtect
from flask_session import Session
from config import config_dict


# 构建数据库对象
db = SQLAlchemy()

# 构建redis连接对象
redis_store = None

# 为flask补充csrf防护机制
csrf = CSRFProtect()


# 工厂模式
def create_app(config_name):
    """创建flask应用对象"""
    app = Flask(__name__)

    conf = config_dict[config_name]

    # 设置flask的配置信息
    app.config.from_object(conf)

    # 初始化数据库db
    db.init_app(app)

    # 初始化
    csrf.init_app(app)

    # 将flask里的session数据保存到redis中
    Session(app)

    # 向app中添加自定义的路由转换器
    # app.url_map.converters["re"] = RegexConverter


    # 注册蓝图
    from . import api_1_0
    app.register_blueprint(api_1_0.api, url_prefix="/api/v1_0")

    # 提供html静态文件的蓝图
    # import web_html
    # app.register_blueprint(web_html.html)

    return app
