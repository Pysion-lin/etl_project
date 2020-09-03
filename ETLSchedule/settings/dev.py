import sys,os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

print(BASE_DIR)
# 添加任务队列
ALL_QUEUE = [
    {"job":"Medical.etl.workscheduler.task_queue.func","parameter":{"trigger":"interval","seconds":5,"data":None,"job_id":"task_log"}},
    # {"job":"workscheduler.task_queue.func_test","parameter":{"trigger":"date","seconds":5,"data":None,"job_id":"task_date"}},

]

# 创建数据库连接(任务存储端)
# DATABASE_URL = "mysql+pymysql://root:12345678@192.168.11.61:3306/{}?charset=utf8".format("pyetl")
DATABASE_URL = "mysql+pymysql://root:longseemysql1!@192.168.1.103:3306/{}?charset=utf8".format("db_etl")

# 创建数据库连接(输入端)
DATABASE_URL_INPUT = r"mssql+pymssql://test:test@192.168.1.100\sql2008/CRM"

# 创建SQL Server连接
SQLSERVER_URL = ""

# 更新数据的前几天
Update_DATE = 3

