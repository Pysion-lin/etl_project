from ETLSchedule.Schedule.workscheduler import ApSchedulerProcess
from ETLSchedule.utils import UpdateResource,UpdateTaskScheduleStatus
from ETLSchedule.task import task_queue


def main():

    scheduler = ApSchedulerProcess()  # 初始化调度器
    scheduler.start()  # 启动任务调度器
    UpdateResource.update_resource()  # 更新功能模块
    UpdateTaskScheduleStatus.update_task_schedule_status()  # 初始化任务计划的状态
    task_queue.run(scheduler)  # 启动任务监控器


if __name__ == '__main__':
    main()  # 启动程序


