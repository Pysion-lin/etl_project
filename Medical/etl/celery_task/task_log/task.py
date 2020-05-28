from celery_task.main import app


@app.task(bind=True,name="process_log")
def send_sms_code(self,mobile,sms_code,time):

    try:
        # ccp = CCP()
        # result = ccp.send_template_sms(mobile, [sms_code, time], 1)
        pass
    except Exception as e:
        result = -1

    if result == -1:
        #参数1: 重试的次数到了之后报错信息, 参数2: 隔几秒发一次,  参数3: 重新发送几次
        self.retry(exc=Exception("最终没有成功"),countdown=5,max_retries=3)