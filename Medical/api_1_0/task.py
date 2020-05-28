from . import api
from Medical.models import User
from flask import current_app,jsonify,request,g
from Medical import csrf,db
from Medical.models import ExtractModel,TransformModel,LoaderModel,TaskModel
from Medical.etl.extracter.extract import Extract
from Medical.etl.loader.loader import LoadData
from Medical.etl.transformer import BaseTransForm
from Medical.etl.task import BaseTask
from Medical.api_1_0.utils.verify_task import Verify
import traceback,random, datetime


@api.route("/extracter",methods=["get"])  # 查询所有的数据输入方法
def extracter():
    try:
        extracts = ExtractModel.query.all()
        extract_app = []
        for extract in extracts:
            extract_app.append(extract.to_dict())
        return jsonify({"status":1,"data":extract_app})
    except Exception as e:
        return jsonify({"status":0,"data":e.__str__()})


@api.route("/transform",methods=["get"])  # 查询所有的数据转换方法
def transform():
    try:
        transforms = LoaderModel.query.all()
        transform_app = []
        for transform_ in transforms:
            transform_app.append(transform_.to_dict())
        return jsonify({"status": 1, "data": transform_app})
    except Exception as e:
        return jsonify({"status":0,"data":e.__str__()})


@api.route("/loader",methods=["get"])  # 查询所有的数据输出方法
def loader():
    try:
        loaders = TransformModel.query.all()
        loaders_app = []
        for load in loaders:
            loaders_app.append(load.to_dict())
        return jsonify({"status": 1, "data": loaders_app})
    except Exception as e:
        return jsonify({"status":0,"data":e.__str__()})


@csrf.exempt
@api.route("/task",methods=["POST","GET"])  # 任务提交接口
def task():
    if request.method == "POST":
        try:
            dict_data = request.get_json()
            print(dict_data)
            if not dict_data.get("extracter") or not dict_data.get("transform") or \
                    not dict_data.get("loader") or not dict_data.get("scheduler") or not dict_data.get("name"):
                raise ValueError("参数不完整")
            extract = Extract()
            load = LoadData()
            transform = BaseTransForm()
            # 校验参数
            try:
                extract_func = getattr(extract,dict_data["extracter"]["name"])
                transform_func = getattr(transform,dict_data["transform"]["name"])
                loader_func = getattr(load,dict_data["loader"]["name"])
            except Exception as e:
                raise ValueError("处理函数不存在")

            # verify = Verify()
            # verify.get_parameter(extract_func)

            instance = TaskModel.query.filter_by(name=dict_data.get("name")).first()
            if not instance:
                task = TaskModel(name=dict_data.get("name"),loader=dict_data["loader"]["name"],transform=dict_data["transform"]["name"],
                          extract=dict_data["extracter"]["name"],scheduler=str(dict_data["scheduler"]["seconds"]),loader_parameter=str(dict_data["loader"]["args"]),
                          transform_parameter=str(dict_data["transform"]["args"]),extract_parameter=str(dict_data["extracter"]["args"]),status=0)
                db.session.add(task)
            else:
                TaskModel.query.filter_by(name=dict_data.get("name")).update({"loader":dict_data["loader"]["name"],"transform":dict_data["transform"]["name"],
                                          "extract":dict_data["extracter"]["name"],"scheduler":str(dict_data["scheduler"]["seconds"]),"loader_parameter":str(dict_data["loader"]["args"]),
                                          "transform_parameter":str(dict_data["transform"]["args"]),"extract_parameter":str(dict_data["extracter"]["args"]),"status":0})
            db.session.commit()
            return jsonify({"status":1,"data":"任务提交成功"})
        except ValueError as v:
            db.session.rollback()
            traceback.print_exc()
            return jsonify({"status":0,"data":v.__str__()})
        except Exception as e:
            traceback.print_exc()
            db.session.rollback()
            return jsonify({"status":0,"data":e.__str__()})

    if request.method == "GET":
        try:
            tasks = TaskModel.query.all()
            task_app = []
            for task in tasks:
                task_app.append(task.to_dict())
            return jsonify({"status":1,"data":task_app})
        except Exception as e:
            return jsonify({"status":0,"data":e.__str__()})


@csrf.exempt
@api.route("/task/scheduler",methods=["POST","GET"])  # 任务提交接口
def task_scheduler():
    try:
        if request.method == "POST":
            dict_data = request.json
            if not dict_data.get("id"):
                raise ValueError("参数不完整,缺少id")
            id = int(dict_data.get("id"))
            # 提取任务处理模块
             ## 提取extract模块
            base_task = BaseTask()
            task = TaskModel.query.filter_by(id=id).first()
            extract_name = task.extract
            transform_name = task.transform
            loader_name = task.loader
            extracter_func = getattr(base_task,extract_name)
              ## 提取transform模块
            transform_func = getattr(base_task, transform_name)
              ## 提取loader模块
            loader_func = getattr(base_task, loader_name)

            # 组装一个任务
            def new_task(_extracter_func,_transform_func,_loader_func,extract_parameter,transform_parameter):
                print("任务开始")
                1/0
                # dataframe = _extracter_func(**dict(eval(extract_parameter)))
                # transform_parameter = dict(eval(transform_parameter))
                # transform_parameter["dataframe"] = dataframe
                # _dataframe = _transform_func(**transform_parameter)
                # result = _loader_func(_dataframe)
            # 添加一个任务

            # 生成随机的任务id
            random_id = datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d%H%M%S") + "%00d"%(random.randint(000,999))
            try:
                scheduler = current_app.config.get("SCHEDULER")
                scheduler.add_job(new_task,trigger="interval",seconds=int(task.scheduler),data=[extracter_func,transform_func,loader_func,task.extract_parameter,task.transform_parameter],job_id=random_id)
            except Exception as e:
                raise ValueError("任务启动失败,请查询错误日志: {}".format(e.__str__()))
            task.status = 1
            task.task_id = random_id
            db.session.commit()
            return jsonify({"status": 1, "data": "任务启动成功"})
    except ValueError as v:
        traceback.print_exc()
        task.status = 0
        db.session.commit()
        return jsonify({"status": 0, "data": v.__str__()})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status":0,"data":e.__str__()})