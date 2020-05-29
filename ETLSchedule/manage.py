from ETLSchedule.Schedule.workscheduler import ApSchedulerProcess
from ETLSchedule.utils import UpdateResource

# 程序入口函数
# def main():
#     try:
#         # import models  # 初始化数据库连接
#         setting_queue = dev.ALL_QUEUE
#         scheduler = ApSchedulerProcess()  # 初始化调度器
#         for path in setting_queue:
#             module_path, class_name = path.get("job").rsplit('.', 1)
#             modle = importlib.import_module(module_path)
#             cls = getattr(modle, class_name)
#             parameter = path.get("parameter")
#             if not cls:
#                 raise ValueError("任务不存在")
#             run(scheduler,cls,parameter)
#         scheduler.start()  # 启动调度器任务
#     except ValueError as v:
#         print(v.__str__())
#     except Exception as e:
#         traceback.print_exc()
#
#
# # 添加任务作业
# def run(scheduler,cls,parameter):
#     scheduler.add_job(cls,**parameter)


def main():

    scheduler = ApSchedulerProcess()  # 初始化调度器
    scheduler.start()  # 启动任务调度器
    UpdateResource.update_resource()  # 更新功能模块
    scheduler.run()  # 启动任务监控器


if __name__ == '__main__':
    main()  # 启动程序


