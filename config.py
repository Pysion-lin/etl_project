

class Config(object):
    """工程的配置信息"""
    SECRET_KEY = "xhosido*F(DHSDF*D(SDdslfhdos"

    # 数据库的配置信息 mysql
    SQLALCHEMY_DATABASE_URI = "mysql+pymysql://root:12345678@192.168.11.31:3306/pyetl"
    SQLALCHEMY_TRACK_MODIFICATIONS = True
    WTF_CSRF_EXEMPT_LIST = ["*"]
    from Medical.etl.manage import scheduler
    SCHEDULER = scheduler


class DevelopmentConfig(Config):
    """开发模式使用的配置信息"""
    DEBUG = True


class ProductionConfig(Config):
    """生产模式 线上模式的配置信息"""
    pass


config_dict = {
    "develop": DevelopmentConfig,
    "product": ProductionConfig
}