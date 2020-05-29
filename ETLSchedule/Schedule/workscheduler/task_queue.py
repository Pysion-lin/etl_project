# from Medical.etl.task.task_log import LogTask
import traceback


# 定义日志处理任务并添加到setting任务列表中
def func(args):
    try:
        print("i am log task")
    except Exception as e:
        traceback.print_exc()


def func_test(args):
    print("i am log1 task")





