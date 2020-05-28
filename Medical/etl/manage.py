from Medical.etl.settings import dev
import importlib,traceback
from Medical.etl.workscheduler import ApSchedulerProcess


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

scheduler = ApSchedulerProcess()  # 初始化调度器


def main():
    scheduler.start()


if __name__ == '__main__':
    main()  # 启动程序


