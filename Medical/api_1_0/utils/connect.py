import traceback
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# 导入相应的模块


class TestConnect(object):
    def __init__(self):
        self.session = None
        self.sqlserver_session = None

    def mysql_connect(self,mysql_database_url):
        engine = create_engine(mysql_database_url, max_overflow=5)  # 创建项目数据库连接，max_overflow指定最大连接数
        dbSession = sessionmaker(engine)  # 创建项目数据库DBSession类型
        self.session = dbSession()  # 创建项目数据库session对象

    def sqlserver_connect(self,sqlserver_database_url):
        engine_sql_server = create_engine(sqlserver_database_url, deprecate_large_types=True)
        sqlserver_db_session = sessionmaker(bind=engine_sql_server)  # 创建项目数据库DBSession类型
        self.sqlserver_session = sqlserver_db_session()  # 创建项目数据库session对象

    def file_connect(self):
        pass

    def mysql_test(self,parameter):
        # mysql_database_url = parameter["description"]
        user = parameter["description"].get("user")
        password = parameter["description"].get("password")
        ip = parameter["description"].get("ip")
        port = parameter["description"].get("port")
        database = parameter["description"].get("database")
        table = parameter["table"]
        # {"description": {"database": "pyetl","ip": "192.168.1.100","password": "12345678","port": 3306,
        # "user": "root"},"id": 1,"type": "SQLserver","sql":"","table":""}
        mysql_database_url = "mysql+pymysql://{user}:{password}@{ip}:{port}/{database}?charset=utf8".format(
            user=user,password=password,ip=ip,port=port,database=database)
        sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '{table}';".format(table=table)
        self.mysql_connect(mysql_database_url)
        if not self.session:
            raise ValueError("mysql数据库连接创建失败")
        try:
            cursor = self.session.execute(sql)
            result = cursor.fetchall()
            self.session.commit()
            return result
        except Exception as e:
            traceback.print_exc()
            raise ValueError("mysql数据库连接失败,{}".format(e.__str__()))

    def sqlserver_test(self,parameter):
        user = parameter["description"].get("user")
        password = parameter["description"].get("password")
        ip = parameter["description"].get("ip")
        database = parameter["description"].get("database")
        table = parameter["table"]
        # DATABASE_URL_INPUT = r"mssql+pymssql://test:test@192.168.1.100\sql2008/CRM"
        sqlserver_database_url = r"mssql+pymssql://{user}:{password}@{ip}/{database}".format(
            user=user,password=password,ip=ip,database=database)
        sql = "SELECT NAME FROM SYSCOLUMNS WHERE ID=OBJECT_ID('{table}');".format(table=table)
        self.sqlserver_connect(sqlserver_database_url)
        if not self.sqlserver_session:
            raise ValueError("sqlserver数据库连接创建失败")
        try:
            cursor = self.sqlserver_session.execute(sql)
            result = cursor.fetchall()
            self.sqlserver_session.commit()
            return result
        except Exception as e:
            traceback.print_exc()
            raise ValueError("sqlserver数据库连接读取失败,{}".format(e.__str__()))