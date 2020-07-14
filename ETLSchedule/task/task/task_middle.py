import traceback,datetime
import pandas as pd
from utils.Logger import logger
from utils import filter_df_for_date


# 组装任务(该方法处理mapping功能)
def get_task(data_dict,task_id,update_type):
    from ETL.extracter.extract import Extract
    from ETL.transformer.trannform import BaseTransForm
    from ETL.loader.loader import LoadData
    import time
    from sqlalchemy.sql import schema
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from settings.dev import DATABASE_URL
    engine = create_engine(DATABASE_URL, max_overflow=5)  # 创建项目数据库连接，max_overflow指定最大连接数
    DBSession = sessionmaker(engine)  # 创建项目数据库DBSession类型
    session = DBSession()  # 创建项目数据库session对象
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)

    try:
        # data_dict = parse_task_parameter(task)
        start = time.time()
        source = eval(data_dict["source"])
        target,primary_key = eval(data_dict["target"]),eval(
            data_dict["primary_key"])
        extract, loader, transform= Extract(), LoadData(), BaseTransForm(),
        if int(source.get("type")) == 2:
            source_connect,from_sql = source.get('connect'),source.get('sql')
            data_frame = extract.read_sqlserver(from_sql, source_connect)
        elif int(source.get("type")) == 1:
            source_connect, from_sql = source.get('connect'), source.get('sql')
            data_frame = extract.read_mysql(from_sql, source_connect)
        elif int(source.get("type")) == 3:
            file_path = source.get('file_path')
            data_frame = extract.read_excel(file_path)
        else:
            raise Exception("数据源不符合")
        methods = eval(data_dict["methods"])
        columns = data_frame.columns.to_list()
        if update_type == 2:  # 是否增量更新
            if "CREATE_TIME" in columns:
                df = filter_df_for_date.filter_df("CREATE_TIME",data_frame)  # 只更新前几天
            elif "ACCEPT_DATE" in columns:
                df = filter_df_for_date.filter_df("ACCEPT_DATE", data_frame)  # 只更新前几天
            else:
                df = data_frame
        else:
            df = data_frame
        # 检查target是否存在
        mapping_columns = []
        target_columns = []
        source_columns = []
        for method in methods:
            for k, v in method.items():
                df = df.where(df.notnull(), None)
                func = getattr(transform, k)
                df = func(df, **v)
                if v.get('column'):
                    source_column = v.get('column')
                elif v.get('virtual_column'):
                    source_column = 'virtual_column'
                else:
                    source_column = None
                if v.get("to_column"):
                    target_column = v.get("to_column")
                else:
                    target_column = None
                if source_column and target_column:
                    if {source_column:target_column} not in mapping_columns:
                            target_columns.append(target_column)
                            source_columns.append(source_column)
                            mapping_columns.append({source_column:target_column})
        if mapping_columns:  # 该任务存在至少一个映射的关系函数
            if len(df) > 0:
                df = change_source_hearder_target(df,mapping_columns,source_columns,target_columns)
                df = change_data_type(df)
                loader.sql_to_mysql(df, target, primary_key, extract, schema, logger)
        else:
            raise Exception("该任务至少需要一个映射函数")
        end = time.time()
        print("使用时间:", end - start)
        change_task_scheduler_status(session, task_id, "任务正常结束,本次花费时间:%s 秒"% int(end-start), 2)
    except Exception as e:
        traceback.print_exc()
        print("任务运行失败:{}".format(e.__str__()))
        change_task_scheduler_status(session,task_id,e.__str__(),-1)


# 转换df的数据类型
def change_data_type(df):
    df = df.where(df.notnull(), None)
    return df


# 任务执行失败时,将对应任务状态修改为出错
def change_task_scheduler_status(session,task_id,e,status):
    from models.models import TaskScheduleModel
    try:
        scheduler = session.query(TaskScheduleModel).filter_by(TaskID=task_id).first()
        if scheduler:
            scheduler.status = status
            scheduler.logs = e
        session.commit()
    except Exception as e:
        traceback.print_exc()


# 修改所有源列名的映射关系并修改改为目标的列名
def change_source_hearder_target(source_df,mapping_columns,source_columns,target_columns):
    # all_source_column = source_df.columns.values.tolist()
    if not mapping_columns:
        raise Exception("该任务没有目标列,请检查任务的功能函数")
    if not source_columns or not source_columns:
        raise Exception("source,target 不存在")
    source_df = source_df[source_columns]
    df_copy = source_df.copy()
    for mapping in mapping_columns:
        df_copy.rename(columns=mapping,inplace=True,copy=False)
    df_copy = df_copy[target_columns]
    return df_copy