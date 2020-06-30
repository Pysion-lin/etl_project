from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.executors.pool import ThreadPoolExecutor,ProcessPoolExecutor
import datetime, traceback, threading, os, sys, gc
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
import logging


class ApSchedulerProcess(object):
    def __init__(self, pools=5):
        self.pools = pools
        # self.executors = {'default': ThreadPoolExecutor(int(self.pools))}
        self.executors = {
        'default': ThreadPoolExecutor(20),
        'processpool': ProcessPoolExecutor(int(self.pools))}
        self.scheduler = BackgroundScheduler(executors=self.executors)
        # self.scheduler = BlockingScheduler(executors=self.executors)
        # 创建监听，任务出错和任务正常结束都会执行job_listener函数
        self.scheduler.add_listener(self.job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)
        logging.basicConfig(level=logging.INFO, format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
                            ,datefmt='%Y-%m-%d %H:%M:%S',filename='scheduler_log.txt',filemode='a')
        self.scheduler._logger = logging

    def add_job(self,job,trigger="interval",seconds=None, data=None, job_id="interval_task"):
        '''
        cron: sched.add_job(job_function, 'cron', month='6-8,11-12', day='3rd fri', hour='0-3')
        interval: sched.add_job(job_function, 'interval', hours=2)
        date: sched.add_job(my_job, 'date', run_date=date(2009, 11, 6), args=['text'])
        '''
        self.id = job_id
        if not seconds:
            self.seconds = 10
        else:
            self.seconds = seconds

        self.data = data
        if self.scheduler:
            if trigger == "interval":
                self.scheduler.add_job(job, trigger='interval', seconds=self.seconds, id=self.id,args=self.data,next_run_time=datetime.datetime.now())
            elif trigger == "cron":
                self.scheduler.add_job(job, trigger='cron', month='6-8,11-12', day='3rd fri', hour='0-3', id=self.id, args=self.data)
            elif trigger == "date":
                date = (datetime.datetime.now() + datetime.timedelta(seconds=seconds)) .strftime("%Y-%m-%dT%H:%M:%S")
                self.scheduler.add_job(job, trigger='date', run_date=date, id=self.id, args=self.data)
        else:
            self.scheduler.logger.error("调度器创建失败")
            raise ValueError("调度器创建失败")

    def start(self):
        if self.scheduler:
            for job in self.scheduler.get_jobs():
                print('job.id:',job.id)
            self.scheduler.start()

    def resume(self,job_id):
        if self.scheduler:
            self.scheduler.resume_job(job_id)

    def pause(self,job_id):
        if self.scheduler:
            self.scheduler.pause_job(job_id)

    def shutdown(self):
        if self.scheduler:
            self.scheduler.shutdown()

    def func(self, data):
        dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("开始执行: %s" % dt, data)

    def job_listener(self,Event):
        '''任务出错时,执行该函数'''
        job = self.scheduler.get_job(Event.job_id)
        args = job.args
        # 正常结束任务
        if not Event.exception:
            # 恢复原先的任务定时时间
            # self.scheduler.reschedule_job(Event.job_id, trigger='cron', hour='00', minute='10', second='00')
            # print('*' * 20, '成功', '*' * 20)
            # for job in self.scheduler.get_jobs():
            #     print(job.name)
            #     print(job.trigger)
            print("任务没有出错,正在运行...")
        else:
            # 计算当前时间5秒后的时间
            # next_datetime = datetime.datetime.now() + datetime.timedelta(seconds=5)
            # 修改出现异常的任务的定时，重新计算下次执行时间，本例为5秒后
            # self.scheduler.reschedule_job(Event.job_id, trigger='cron', hour=next_datetime.hour, minute=next_datetime.minute,
            #                      second=next_datetime.second)
            msg = f"jobname={job.name}|jobtrigger={job.trigger}|errcode={Event.code}|exception=[{Event.exception}]|traceback=[{Event.traceback}]|scheduled_time={Event.scheduled_run_time}"
            print("任务出错,出错信息:{}".format(msg))
            self.scheduler.logger.error(msg)

    def run(self):
        import time
        while True:
            jod_store = self.scheduler.get_jobs()

            print("当前的任务: {}".format(jod_store))

            # TODO 查询数据库中任务表的数据是否有更新,根据更新的数据添加任务到任务队列中
            time.sleep(2)


if __name__ == '__main__':
    def fun(data):
        print(data)
    instrumenlog = ApSchedulerProcess()
    instrumenlog.add_job(fun,seconds=1,data="ok",job_id="test")
    instrumenlog.add_job(fun,seconds=1,data="ok1",job_id="test1")
    instrumenlog.add_job(fun,seconds=1,data="ok2",job_id="test2")
    instrumenlog.start()
    # 创建监听，任务出错和任务正常结束都会执行job_listener函数
    # sched.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)