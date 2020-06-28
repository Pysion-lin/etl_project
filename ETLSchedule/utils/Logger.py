import logging
from logging import handlers
import os,sys

level_relations = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'crit':logging.CRITICAL
    }  # 日志级别关


def logger(filename=None,level='info',when='D',backCount=0,fmt='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s'):
    if not filename:
        filename = os.path.join(sys.path[0], "logs", "%s" % "INFO.log")
    else:
        filename = filename  # os.path.join(sys.path[0], "logs/%s" % filename)
    logger = logging.getLogger(filename)
    format_str = logging.Formatter(fmt)  # 设置日志格式
    logger.setLevel(level_relations.get(level))  # 设置日志级别
    if not logger.handlers:
        sh = logging.StreamHandler()  # 往屏幕上输出
        sh.setFormatter(format_str)  # 设置屏幕上显示的格式
        th = handlers.TimedRotatingFileHandler(filename=filename, when=when, backupCount=backCount,
                                               encoding='utf-8')  # 往文件里写入#指定间隔时间自动生成文件的处理器
        chil_instance = th
        chil_instance_ = sh
        chil_instance.setFormatter(format_str)  # 设置文件里写入的格式
        # self.logger.addHandler(self.chil_instance_)  # 把对象加到logger里
        logger.addHandler(chil_instance)
    return logger
    #  实例化TimedRotatingFileHandler
    #  interval是时间间隔，backupCount是备份文件的个数，如果超过这个个数，就会自动删除，when是间隔的时间单位，单位有以下几种：
    # S 秒
    # M 分
    # H 小时、
    # D 天、
    # W 每星期（interval==0时代表星期一）
    # midnight 每天凌晨


if __name__ == '__main__':
    # log = Logger('all.log',level='debug')
    # log.logger.debug('debug')
    # log.logger.info('info')
    # log.logger.warning('警告')
    # log.logger.error('报错')
    # log.logger.critical('严重')
    # Logger('error.log', level='error').logger.error('error')
    pass
