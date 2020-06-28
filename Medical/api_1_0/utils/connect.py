import traceback
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import MetaData, inspect,Table
# 导入相应的模块


class TestConnect(object):
    def __init__(self):
        self.session = None
        self.sqlserver_session = None
        self.engine = None
        self.engine_sql_server = None

    def get_table(self,sql):
        return str(str(sql).partition("from")[2]).split(" ")[1]

    def get_mysql_field_from_engin(self,table):
        md = MetaData()
        table = Table(table, md, autoload=True, autoload_with=self.engine)
        columns = table.c
        return [c.name for c in columns]

    def get_sqlserver_field_from_engin(self,table):
        md = MetaData()
        table = Table(table, md, autoload=True, autoload_with=self.sqlserver_session)
        columns = table.c
        return [c.name for c in columns]

    def mysql_connect(self,mysql_database_url):
        self.engine = create_engine(mysql_database_url, max_overflow=5)  # 创建项目数据库连接，max_overflow指定最大连接数
        dbSession = sessionmaker(self.engine)  # 创建项目数据库DBSession类型
        self.session = dbSession()  # 创建项目数据库session对象

    def sqlserver_connect(self,sqlserver_database_url):
        self.engine_sql_server = create_engine(sqlserver_database_url, deprecate_large_types=True)
        sqlserver_db_session = sessionmaker(bind=self.engine_sql_server)  # 创建项目数据库DBSession类型
        self.sqlserver_session = sqlserver_db_session()  # 创建项目数据库session对象

    def file_connect(self):
        pass

    def mysql_connect_test(self,parameter):
        user = parameter["user"]
        password = parameter["password"]
        ip = parameter["ip"]
        port = parameter["port"]
        database = parameter["database"]
        mysql_database_url = "mysql+pymysql://{user}:{password}@{ip}:{port}/{database}?charset=utf8".format(
            user=user, password=password, ip=ip, port=port, database=database)
        print('mysql_database_url',mysql_database_url)
        self.mysql_connect(mysql_database_url)
        self.mysql_test()

    def mysql_test(self):
        if not self.engine:
            raise ValueError("数据库连接创建失败")
        try:
            self.engine.connect()
        except Exception as e:
            raise ValueError("数据库连接创建失败:{}".format(e.__str__()))

    def mysql_get_sql_field(self,sql):
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

    def mysql_get_table_field(self,table):
        sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '{table}';".format(
            table=table)
        return self.mysql_get_sql_field(sql)

    def sqlserver_connect_test(self,parameter):
        user = parameter["user"]
        password = parameter["password"]
        ip = parameter["ip"]
        database = parameter["database"]
        # DATABASE_URL_INPUT = r"mssql+pymssql://test:test@192.168.1.100\sql2008/CRM"
        sqlserver_database_url = r"mssql+pymssql://{user}:{password}@{ip}/{database}".format(
            user=user, password=password, ip=ip, database=database)
        self.sqlserver_connect(sqlserver_database_url)
        self.sqlserver_test()

    def sqlserver_get_table_field(self, table):
        sql = "SELECT NAME FROM SYSCOLUMNS WHERE ID=OBJECT_ID('{table}');".format(table=table)
        return [list(filed)[0] for filed in self.sqlserver_get_sql_field(sql)]

    def sqlserver_test(self):
        if not self.engine_sql_server:
            raise ValueError("数据库连接创建失败")
        try:
            self.engine_sql_server.connect()
        except Exception as e:
            raise ValueError("数据库连接创建失败:{}".format(e.__str__()))

    def sqlserver_get_sql_field(self,sql):
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