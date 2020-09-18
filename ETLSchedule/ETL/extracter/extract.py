import pandas as pd
import traceback

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


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

    def read_excel(self,file_path):
        '''读取指定的Excel文件'''
        if not file_path:
            raise ValueError("请提供文件路径")
        try:
            ret = pd.read_excel(file_path)
        except Exception as e:
           raise ValueError(e.__str__())
        return ret


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
            df = pd.DataFrame()
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
            traceback.print_exc()
            df = pd.DataFrame()
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

