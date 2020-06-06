from ETLSchedule.models import session
from ETLSchedule.models.models import TransformModel, TaskModel, TaskScheduleModel
import traceback
from ETLSchedule.ETL.extracter.extract import Extract
from ETLSchedule.ETL.transformer.trannform import BaseTransForm
from ETLSchedule.ETL.loader.loader import LoadData
from sqlalchemy.exc import InternalError


def run(scheduler):
    try:
        import time
        while True:
            jod_store = scheduler.scheduler.get_jobs()
            print("当前启动的任务列表: {}".format(jod_store))
            # 获取所有等待启动的task
            task_app = get_start_task()
            # 获取task
            for task in task_app:
                start_task(scheduler, task["task"], task["interval"], task["TaskID"], task["task_scheduler"])
            # 修改task的status
            pause_resume_task_scheduler(scheduler,get_task_scheduler_status())

            # TODO 查询数据库中任务表的数据是否有更新,根据更新的数据添加任务到任务队列中
            time.sleep(2)
    except TypeError as t:
        traceback.print_exc()
        print(t.__str__())
    except Exception as e:
        traceback.print_exc()
    except InternalError as I:
        traceback.print_exc()
        print(I.__str__())


# 获取所有的任务计划列表
def get_task_scheduler():
    task_schedulers = session.query(TaskScheduleModel).all()
    session.commit()
    return task_schedulers


# 获取所有任务计划的任务id和对应的状态
def get_task_scheduler_status():
    scheduler_status_app = []
    task_schedulers = get_task_scheduler()
    for task_scheduler in task_schedulers:
        scheduler_status_app.append({task_scheduler.TaskID:task_scheduler.status})
    return scheduler_status_app


# 停止/暂停 任务计划
def pause_resume_task_scheduler(scheduler, scheduler_status):
    all_jobs = get_all_running_task_id(scheduler)
    for task_schedule in scheduler_status:
        for task_id,status in task_schedule.items():
            if task_id in all_jobs:
                if status == 0:  # 暂停
                    scheduler.scheduler.pause_job(task_id)
                    # change_task_scheduler_status(session,task_id,"任务正常暂停",0)
                if status == -1:
                    scheduler.scheduler.pause_job(task_id)
                    # change_task_scheduler_status(session, task_id, "任务出错暂停", 0)
                if status == 1:
                    scheduler.scheduler.resume_job(task_id)
                    # change_task_scheduler_status(session, task_id, "任务重新启动", 2)


# 获取所有要启动的计划任务
def get_start_task():
    try:
        task_app = []
        task_schedulers = get_task_scheduler()
        for task_scheduler in task_schedulers:
            if task_scheduler.status == 1:
                # print('task_scheduler.task_id',task_scheduler.task_id)
                task_app.append({"task": session.query(TaskModel).filter_by(id=task_scheduler.task_id).first(),
                                 "interval": int(task_scheduler.schedule), "TaskID": task_scheduler.TaskID,
                                 "task_scheduler": task_scheduler})
        return task_app
    except Exception as e:
        traceback.print_exc()


# 解析任务的所有参数
def parse_task_parameter(task):
    dict_data = task.to_dict()
    return dict_data


# 组装任务(该方法处理mapping功能)
def get_task(data_dict,task_id):
    from sqlalchemy.sql import schema
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from ETLSchedule.settings.dev import DATABASE_URL
    engine = create_engine(DATABASE_URL, max_overflow=5)  # 创建项目数据库连接，max_overflow指定最大连接数
    DBSession = sessionmaker(engine)  # 创建项目数据库DBSession类型
    session = DBSession()  # 创建项目数据库session对象

    try:
        # data_dict = parse_task_parameter(task)
        source = eval(data_dict["source"])
        target, source_connect, primary_key = eval(data_dict["target"]), source.get('connect'), eval(
            data_dict["primary_key"])
        extract, loader, transform, from_sql = Extract(), LoadData(), BaseTransForm(), source.get('sql')
        data_frame = extract.read_sqlserver(from_sql, source_connect)
        methods = eval(data_dict["methods"])
        df = data_frame
        # 检查target是否存在
        mapping_columns = []
        target_columns = []
        source_columns = []
        for method in methods:
            for k, v in method.items():
                func = getattr(transform, k)
                df = func(df, **v)
                if {v["column"]:v["to_column"]} not in mapping_columns:
                    target_columns.append(v["to_column"])
                    source_columns.append(v["column"])
                    mapping_columns.append({v["column"]:v["to_column"]})
        df = change_source_hearder_target(df,mapping_columns,source_columns,target_columns)
        loader.sql_to_mysql(df, target, primary_key, extract,schema)
        change_task_scheduler_status(session, task_id, "任务正常运行中...", 2)
    except Exception as e:
        traceback.print_exc()
        print("任务运行失败:{}".format(e.__str__()))
        change_task_scheduler_status(session,task_id,e.__str__(),-1)


# 任务执行失败时,将对应任务状态修改为出错
def change_task_scheduler_status(session,task_id,e,status):
    from ETLSchedule.models.models import TaskScheduleModel
    try:
        print(session,task_id,e,status)
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
    source_df = source_df[source_columns]
    df_copy = source_df.copy()
    for mapping in mapping_columns:
        df_copy.rename(columns=mapping,inplace=True,copy=False)
    df_copy = df_copy[target_columns]
    return df_copy


# 获取所有正在运行的任务id
def get_all_running_task_id(scheduler):
    task_id_app = []
    job_store = scheduler.scheduler.get_jobs()
    for job in job_store:
        task_id_app.append(job.id)
    return list(task_id_app)


# 添加任务到任务调度器中并将任务计划的状态重置为2,表示当前任务正在进行
def start_task(scheduler, task, interval, task_id, task_scheduler):
    try:
        jod_store = get_all_running_task_id(scheduler)
        if task_id not in jod_store:
            data_dict = parse_task_parameter(task)
            scheduler.scheduler.add_job(get_task, trigger="interval", seconds=interval, id=task_id, args=[data_dict,task_id])
            task_scheduler.status = 2
            session.commit()
    except Exception as e:
        traceback.print_exc()


if __name__ == '__main__':
    import pandas as pd
    # data = {}
    # df =
    # df = p

