import traceback
from ETLSchedule.utils.Logger import logger


def get_task(data_dict,task_id,update_type):
    from ETLSchedule.ETL.extracter.extract import Extract
    from ETLSchedule.ETL.transformer.trannform import BaseTransForm
    from ETLSchedule.ETL.loader.loader import LoadData
    from sqlalchemy.sql import schema
    import time
    try:
        start = time.time()
        # data_dict = parse_task_parameter(task)
        source = eval(data_dict["source"])
        target, source_connect, primary_key = eval(data_dict["target"]), source.get("connect"), eval(
            data_dict["primary_key"])
        extract, loader, transform, from_sql = Extract(), LoadData(), BaseTransForm(), source.get('sql')
        if int(source.get("type")) == 2:
            data_frame = extract.read_sqlserver(from_sql, source_connect)
        elif int(source.get("type")) == 1:
            data_frame = extract.read_mysql(from_sql, source_connect)
        else:
            raise Exception("数据源不符合")
        methods = eval(data_dict["methods"])
        df = data_frame
        if len(df) < 0:
            raise Exception("无法获取dataframe")
        session = extract.create_mysql_session__(target["connect"])
        target_connect = target["connect"]
        for method in methods:
            for k, v in method.items():
                df = df.where(df.notnull(), None)
                func = getattr(transform, k)
                df = func(df,extract,source_connect,target_connect,session,schema,logger,update_type)
        loader.sql_to_test_mysql(df, target_connect, extract, schema,logger)
        end = time.time()
        print("使用时间:",end-start)
        change_task_scheduler_status(task_id, "任务正常结束,本次花费时间:%s 秒"% int(end-start), 2)
    except Exception as e:
        traceback.print_exc()


# 任务执行失败时,将对应任务状态修改为出错
def change_task_scheduler_status(task_id,e,status):
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from ETLSchedule.settings.dev import DATABASE_URL
    engine = create_engine(DATABASE_URL, max_overflow=5)  # 创建项目数据库连接，max_overflow指定最大连接数
    DBSession = sessionmaker(engine)  # 创建项目数据库DBSession类型
    session = DBSession()  # 创建项目数据库session对象
    from ETLSchedule.models.models import TaskScheduleModel
    try:
        scheduler = session.query(TaskScheduleModel).filter_by(TaskID=task_id).first()
        if scheduler:
            scheduler.status = status
            scheduler.logs = e
        session.commit()
    except Exception as e:
        traceback.print_exc()

if __name__ == '__main__':
    source ='''{"connect": {"database": "db_mid_bigdata", "ip": "192.168.1.100", "password": "longseeuser01", "port": 3306, 
    "user": "user01"}, "id": 2, "sql": "select * from personal_acceptinfo;", "type": 1}'''
    # target = "{'connect': {'database': 'db_mid_bigdata', 'ip': '192.168.1.100', 'password': 'longseeuser01'," \
    #          " 'port': 3306, 'user': 'user01'}, 'id': 2, 'table': 'wj_answer_copy1', 'type': 1}"

    target = "{'connect': {'database': 'db_bigdata', 'ip': '192.168.1.204', 'password': 'longseeuser01'," \
             " 'port': 3306, 'user': 'user01'}, 'id': 2, 'table': 'wj_answer_copy1', 'type': 1}"
    data_dict = {
                 'source': source, 'target': target,
                 'methods': "[{'translate_test': {}}]",
                 'primary_key': "{'to_primary_field': 'ID'}",
                 'name': 'vw_wj_answer_master'
                 }
    get_task(data_dict,'')



