from models import session
from models.models import TaskModel, TaskScheduleModel
import traceback,datetime
from sqlalchemy.exc import InternalError
from task.task import task_product_personal_info,task_product_test
from task.task import task_middle


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
                start_task(scheduler, task["task"], task["interval"], task["TaskID"], task["task_scheduler"],task["type"],task["update_type"])
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
                    scheduler.scheduler.remove_job(task_id)
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
                                 "task_scheduler": task_scheduler,"type":int(task_scheduler.type),"update_type":task_scheduler.update})
        return task_app
    except Exception as e:
        traceback.print_exc()


# 解析任务的所有参数
def parse_task_parameter(task):
    dict_data = task.to_dict()
    return dict_data


# 获取所有正在运行的任务id
def get_all_running_task_id(scheduler):
    task_id_app = []
    job_store = scheduler.scheduler.get_jobs()
    for job in job_store:
        task_id_app.append(job.id)
    return list(task_id_app)


# 添加任务到任务调度器中并将任务计划的状态重置为2,表示当前任务正在进行
def start_task(scheduler, task, interval, task_id, task_scheduler,task_type,update_type):
    try:
        jod_store = get_all_running_task_id(scheduler)
        if task_id not in jod_store:
            data_dict = parse_task_parameter(task)
            if task_type == 1:  # 到中间库
                scheduler.scheduler.add_job(task_middle.get_task, trigger="interval", seconds=interval,
                                            id=task_id, args=[data_dict,task_id,update_type], next_run_time=datetime.datetime.now())
            elif task_type == 2:  # 到档案库
                scheduler.scheduler.add_job(task_product_personal_info.get_task, trigger="interval", seconds=interval, id=task_id,
                                            args=[data_dict, task_id,update_type], next_run_time=datetime.datetime.now())  # 档案信息
            elif task_type == 3:  # 检测信息库
                scheduler.scheduler.add_job(task_product_test.get_task, trigger="interval", seconds=interval,
                                            id=task_id,
                                            args=[data_dict, task_id,update_type], next_run_time=datetime.datetime.now())  # 检测信息

            task_scheduler.status = 2
            session.commit()
    except Exception as e:
        traceback.print_exc()


if __name__ == '__main__':
    import pandas as pd
    # data = {}
    # df =
    # df = p

