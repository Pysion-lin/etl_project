import pandas as pd
import traceback

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from ETLSchedule.settings.dev import DATABASE_URL,DATABASE_URL_INPUT
# 导入相应的模块
# engine = create_engine(DATABASE_URL, max_overflow=5)  # 创建项目数据库连接，max_overflow指定最大连接数
# DBSession = sessionmaker(engine)  # 创建项目数据库DBSession类型
# session = DBSession()  # 创建项目数据库session对象
#
# engine_SQL_Server = create_engine(DATABASE_URL_INPUT,deprecate_large_types=True)
# SQLServerDBSession = sessionmaker(bind=engine_SQL_Server)  # 创建项目数据库DBSession类型
# SQLServer_session = SQLServerDBSession()  # 创建项目数据库session对象


class Extract(object):
    '''数据抽取基类'''
    def __init__(self):
        self.file_path = None

    def read_file(self,file_path=None,header=None,sep=" = "):
        '''读取指定文件路径的方法,file_path:文件路径,header:是否包含头,sep:数据分割符'''
        if not file_path:
            raise ValueError("请提供文件路径")
        try:
            ret = pd.read_table(file_path,header=header,sep=sep)
        except Exception as e:
           raise ValueError(e.__str__())
        return ret

    # def csv_extract(self,file_path,header=None,sep=" "):
    #     if not file_path:
    #         raise ValueError("请提供文件路径")
    #     try:
    #         ret = pd.read_csv(file_path, header=header, sep=sep)
    #     except Exception as e:
    #         raise ValueError(e.__str__())
    #     return ret
    #
    # def ftp_extract(self):
    #     print("i am ftp")
    #     pass
    #
    # def mysql_extract(self):
    #     print("i am mysql")
    #     pass
    #
    # def url_extract(self):
    #     print("i am url")
    #     pass
    #
    # def read_json(self,url):
    #     if not url:
    #         re = requests.get("http://192.168.1.103:8002/kit/forwardinfo/?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6NSwiZGVwYXJ0bWVudCI6eyJpZCI6MSwibmFtZSI6Ilx1NTkyN1x1NjU3MFx1NjM2ZVx1NGUyZFx1NWZjMyJ9LCJ0ZWwiOiIxODgxNDA5NDA2MCIsIm5hbWUiOiJcdTY3OTdcdTY1ODdcdTY4ZWUiLCJuYW1lX2NvZGUiOiJsb25nc2VlIiwicGFzc3dvcmQiOiJMbDEyMzQ1Njc4OSIsImlzX3N1cGVyIjoxLCJpc19hdWRpdCI6MSwiY3JlYXRlX3RpbWUiOiIyMDIwLTA0LTI4VDAwOjAwOjAwIiwidXBkYXRlX3RpbWUiOiIyMDIwLTA1LTE2VDE2OjM2OjIwIiwiZXhwaXJlcyI6IjIwMjAtMDUtMjIgMTc6NTE6MTgifQ.gwQm_CG3Zlrv3wp2jPDvi5_VMz8z4I5MymCJ7K5A-d8&instrument_id=6")
    #     else:
    #         re = requests.get(url)
    #     try:
    #         json_data = json.dumps(eval(re.text))
    #     except Exception as e:
    #         raise ValueError("通过url获取的的数据不是json格式")
    #     df = pd.read_json(json_data)
    #     return df
    def create_mysql_engin__(self,connect):
        url = "mysql+pymysql://{user}:{password}@{ip}:{port}/{database}?charset=utf8".format(user=connect['user'],password=connect['password'],
                                                                                ip=connect['ip'],port=connect['port'],database=connect['database'])
        engine = create_engine(url, max_overflow=5)  # 创建项目数据库连接，max_overflow指定最大连接数
        return engine

    def create_mysql_session__(self,connect):
        engine = self.create_mysql_engin__(connect)
        DBSession = sessionmaker(engine)  # 创建项目数据库DBSession类型
        session = DBSession()  # 创建项目数据库session对象
        return session

    def read_mysql(self,sql,connect):
        '''读取指定SQLserver数据库的数据,sql:查询表的语句 如:select * from TB_BDM_Employee'''
        engine = self.create_mysql_engin__(connect)
        try:
            with engine.connect() as con, con.begin():
                df = pd.read_sql(sql, con)  # 获取数据
                con.close()
        except Exception as e:
            traceback.print_exc()
            df = None
        return df

    def create_sqlserver_engin__(self, connect):
        url = r"mssql+pymssql://{user}:{password}@{ip}/{database}".format(user=connect['user'],password=connect['password'],
                                                                          ip=connect['ip'],database=connect['database'])
        engine = create_engine(url, deprecate_large_types=True)  # 创建项目数据库连接，max_overflow指定最大连接数
        return engine

    def create_sqlserver_session__(self, connect):
        engine = self.create_sqlserver_engin__(connect)
        DBSession = sessionmaker(engine)  # 创建项目数据库DBSession类型
        session = DBSession()  # 创建项目数据库session对象
        return session

    def read_sqlserver(self, sql,connect):
        '''读取指定SQLserver数据库的数据,sql:查询表的语句 如:select * from TB_BDM_Employee'''
        engine = self.create_sqlserver_engin__(connect)
        try:
            with engine.connect() as con, con.begin():
                df = pd.read_sql(sql, con)  # 获取数据
                con.close()
        except Exception as e:
            df = None
        # print("read_sqlserver:", df)
        return df
        # try:
        #     result = engine_SQL_Server.query(TBEmployeeModel).all()
        #     # context.log.info("the type is {}".format(type(result)))
        #     # context.log.info("result is {}".format(result))
        #     engine_SQL_Server.close()
        #     return result
        # except Exception as e:
        #     traceback.print_exc()
        #     # context.log.error("error info: {}".format(e.__str__()))
        # return result

    def methods__(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))

