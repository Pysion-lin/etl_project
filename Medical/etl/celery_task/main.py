from __future__ import absolute_import, unicode_literals
import os
from celery import Celery

#2,加载环境变量
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "meiduo_mall.settings.dev")

#3,创建celery对象
app = Celery('pytel')

#4,加载配置文件,(任务,结果队列)
app.config_from_object('celery_tasks.config')

#5,注册任务
app.autodiscover_tasks(['celery_tasks.email.tasks',
                        ])

#6,装饰任务
# @app.task(bind=True)
# def hello_world(self):
#     import time
#     for i in range(0,10):
#         time.sleep(1)
#         print("i = %s"%i)