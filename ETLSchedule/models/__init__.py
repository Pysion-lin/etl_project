from sqlalchemy import create_engine,MetaData,Table
from sqlalchemy.orm import sessionmaker
from ETLSchedule.settings.dev import DATABASE_URL,DATABASE_URL_INPUT
# 导入相应的模块
engine = create_engine(DATABASE_URL, max_overflow=5)  # 创建项目数据库连接，max_overflow指定最大连接数
DBSession = sessionmaker(engine)  # 创建项目数据库DBSession类型
session = DBSession()  # 创建项目数据库session对象

engine_SQL_Server = create_engine(DATABASE_URL_INPUT,deprecate_large_types=True)
SQLServerDBSession = sessionmaker(bind=engine_SQL_Server)  # 创建项目数据库DBSession类型
SQLServer_session = SQLServerDBSession()  # 创建项目数据库session对象


if __name__ == '__main__':
    print(SQLServer_session)
    sql = "select top 1 * from TB_BDM_Employee"
    cursor = SQLServer_session.execute(sql)
    result = cursor.fetchall()
    print(result)
    SQLServer_session.close()
