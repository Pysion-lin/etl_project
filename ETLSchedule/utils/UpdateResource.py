from ETLSchedule.ETL.transformer.trannform import BaseTransForm
from .GetFunctionArgs import get_func_args
from ETLSchedule.models import session
from ETLSchedule.models.models import TransformModel,TaskModel
import traceback


def update_resource():
    try:
        transform = BaseTransForm()

        for name in transform.methods__():
            args = get_func_args(name, transform)
            instance = session.query(TransformModel).filter_by(name=name).first()
            tmp_func = getattr(transform, name)
            for property_name,value in vars(transform).items():
                if property_name.find(name) != -1:  # 方法和方法的属性设置
                    if not instance:
                        function_model = TransformModel(name=name,module_id=value["module_id"],args=str(args),is_primary_key=value["primary_key"], description=tmp_func.__doc__)
                        session.add(function_model)
                    else:
                        session.query(TransformModel).filter_by(name=name).update({"args": str(args),"module_id":value["module_id"],"is_primary_key":value["primary_key"],"description": tmp_func.__doc__})
                # else:
                #     print("请配置transform的init属性值,name:{},property_name:{}".format(name,property_name))
                    # Exception("请配置transform的init属性值")
        source = '''{"connect": {"database": "db_mid_bigdata", "ip": "192.168.1.100", "password": "longseeuser01", "port": 3306, 
            "user": "user01"}, "id": 2, "sql": "select * from test_apply where WJID != 'NULL';", "type": 1}'''
        target = "{'connect': {'database': 'db_mid_bigdata', 'ip': '192.168.1.100', 'password': 'longseeuser01'," \
                 " 'port': 3306, 'user': 'user01'}, 'id': 2, 'table': 'wj_answer_copy1', 'type': 1}"
        methods = "[{'translate': {}}]"
        primary_key = "{'to_primary_field': 'ID'}"
        task_instance = session.query(TaskModel).filter_by(name="档案库任务").first()
        if not task_instance:
            task = TaskModel(name="档案库任务",source=source,methods=methods,target=target,primary_key=primary_key)
            session.add(task)
        # else:
        #     session.query(TaskModel).filter_by(name="档案库任务").update({"source":source,"methods":methods,"target":target,"primary_key":primary_key})
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            traceback.print_exc()  # TODO 启动日志记录
    except Exception as e:
        traceback.print_exc()  # TODO 启动日志记录
