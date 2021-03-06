

class Config(object):
    """工程的配置信息"""
    SECRET_KEY = "xhosido*F(DHSDF*D(SDdslfhdos"

    # 数据库的配置信息 mysql

    SQLALCHEMY_TRACK_MODIFICATIONS = True
    WTF_CSRF_EXEMPT_LIST = ["*"]


class DevelopmentConfig(Config):
    """开发模式使用的配置信息"""
    DEBUG = True
    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:12345678@192.168.11.61:3306/pyetl"


class ProductionConfig(Config):
    """生产模式 线上模式的配置信息"""
    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:longseemysql1!@192.168.1.103:3306/db_etl_ai"


config_dict = {
    "develop": DevelopmentConfig,
    "product": ProductionConfig
}