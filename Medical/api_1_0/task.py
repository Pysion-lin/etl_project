from . import api
from flask import current_app, jsonify, request
from Medical import csrf, db
from Medical.models import ExtractModel, TransformModel, LoaderModel, TaskModel, ResourceType,TransformModuleModel,TaskScheduleModel
import traceback, random, datetime
from Medical.api_1_0.utils.connect import TestConnect
from Medical.api_1_0.utils import judge_empty
from Medical.api_1_0.utils import parse_parameter
import pandas as pd


@csrf.exempt
@api.route("/resource", methods=["GET", "POST"])  # get查询所有的可用数据源,post连接测试
def resource():
    # 查询连接资源
    if request.method == "GET":
        try:
            resources = ResourceType.query.all()
            resource_app = []
            for resource in resources:
                data = resource.to_dict()
                data["description"] = eval(data["description"])
                resource_app.append(data)
            return jsonify({"status": 1, "data": resource_app})
        except Exception as e:
            return jsonify({"status": 0, "data": e.__str__()})
    elif request.method == "POST":  # 测试连通性
        try:
            dict_data = request.get_json()  # {"description": {"database": "pyetl","ip": "192.168.1.100","password": "12345678","port": 3306,
            # "user": "root"},"id": 1,"type": "SQLserver","sql":"","table":""}
            print("dict_data",dict_data)
            type_id = dict_data.get("id")
            if not type_id:
                raise ValueError("类型id不存在")
            # 校验参数
            if not all([dict_data.get("connect"), dict_data["id"], dict_data["connect"].get("database"),
                        dict_data["connect"].get("ip"), dict_data["connect"].get("password"),
                        dict_data["connect"].get("user")]):
                raise ValueError("参数不完整")
            connect = TestConnect()
            if int(type_id) == 2:
                if not all([dict_data["connect"].get("port")]):
                    raise ValueError("参数不完整,缺少port参数")
                print('dict_data.get("connect")',dict_data.get("connect"))
                connect.mysql_connect_test(dict_data.get("connect"))
            elif int(type_id) == 1:
                connect.sqlserver_connect_test(dict_data.get("connect"))
            else:
                raise ValueError("类型不存在")
            return jsonify({"status": 1, "data": "数据库连接成功"})
        except ValueError as v:
            traceback.print_exc()
            return jsonify({"status": 0, "data": v.__str__()})
        except Exception as e:
            traceback.print_exc()
            return jsonify({"status": 0, "data": e.__str__()})


@csrf.exempt
@api.route("/field", methods=["GET", "POST"])  # get获取连接池的字段
def field():
    try:
        dict_data = request.json

        if not dict_data:
            dict_data = request.args
        if not dict_data:
            dict_data = request.data
        if not dict_data:
            dict_data = request.form
        if not dict_data:
            raise ValueError("参数不能为空")
        print('data_dict',dict_data)
        type_id = dict_data.get("id")  # mysql/SQLserver
        type_name = dict_data.get("type")  # source/target
        if not type_id:
            raise ValueError("类型id不存在")
        if not type_name:
            raise ValueError("类型名称不存在")
        # 校验参数
        save_path = None
        if request.method == "POST":  # 获取字段
            ret = None
            if type_name == "source":  # 类型为源的字段
                sql = dict_data.get("sql")
                if not sql:
                    raise ValueError("sql不存在")
                sql = sql + " "
                if int(type_id) == 2:  # 获取连接为mysql的字段
                    judge_empty.Judge_Empty(2, dict_data.get("connect"))
                    connect = TestConnect()
                    connect.mysql_connect_test(dict_data.get("connect"))  # 测试是否连通
                    parse_sql = parse_parameter.get_str_btw(sql,"select","from")
                    print("parse_sql",parse_sql,type(parse_sql))
                    if not parse_sql:
                        raise ValueError("输入的SQL语句不符合规范")
                    if "*" in parse_sql:
                        table = connect.get_table(sql)
                        ret = connect.get_mysql_field_from_engin(table)
                    else:
                        ret = parse_sql
                elif int(type_id) == 1: # 获取连接为SQLserver的字段
                    judge_empty.Judge_Empty(2, dict_data.get("connect"))
                    connect = TestConnect()
                    connect.sqlserver_connect_test(dict_data.get("connect"))
                    parse_sql = parse_parameter.get_str_btw1(sql, "select", "from")
                    print(type(parse_sql),parse_sql)
                    if not parse_sql:
                        raise ValueError("输入的SQL语句不符合规范")
                    if "*" in parse_sql:
                        table = connect.get_table(sql)
                        result = connect.sqlserver_get_sql_field("select top 1 * from {}".format(table))
                        ret = [key for key in result[0]._keymap.keys()]
                    else:
                        if "top" in parse_sql:
                            result = connect.sqlserver_get_sql_field(sql)
                            ret = [key for key in result[0]._keymap.keys()]
                        else:
                            ret = parse_parameter.get_str_btw(sql, "select", "from")
            elif type_name == "target":  # 类型为目标的字段
                table = dict_data.get("table")
                if not table:
                    raise ValueError("table不存在")
                if int(type_id) == 2:  # 获取连接为mysql的字段
                    judge_empty.Judge_Empty(2, dict_data.get("connect"))
                    connect = TestConnect()
                    connect.mysql_connect_test(dict_data.get("connect"))  # 测试是否连通
                    ret = connect.get_mysql_field_from_engin(table)
                elif int(type_id) == 1: # 获取连接为SQLserver的字段
                    judge_empty.Judge_Empty(2, dict_data.get("connect"))
                    connect = TestConnect()
                    connect.sqlserver_connect_test(dict_data.get("connect"))
                    ret = connect.sqlserver_get_table_field(table)
                    if not ret:
                        raise ValueError("该表格不存在")
            elif type_name == "Excel":  # 获取连接为Excel方式
                import os,sys
                files = request.files.get("file")
                file = files.read()
                file_name = files.filename
                save_path = os.path.join(sys.path[0],"Medical","api_1_0","tmp",str(file_name))
                with open(save_path,"wb") as f:
                    f.write(file)
                df = pd.read_excel(save_path)
                if len(df) > 0:
                    field = df.columns.to_list()
                    if field:
                        ret = field

            return jsonify({"status": 1, "data": ret,"file_name":save_path})
    except ValueError as v:
        traceback.print_exc()
        return jsonify({"status": 0, "data": v.__str__()})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": 0, "data": e.__str__()})


@api.route("/transform/module/", methods=["get"])  # 查询所有功能函数的所属模块
def transform_module():
    try:
        modules = TransformModuleModel.query.all()
        module_app = []
        for module in modules:
            module_app.append(module.to_dict())
        return jsonify({"status": 1, "data":module_app})
    except ValueError as v:
        traceback.print_exc()
        return jsonify({"status": 0, "data": v.__str__()})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": 0, "data": e.__str__()})


@api.route("/transform", methods=["get"])  # 查询所有的数据转换方法
def transform():
    try:
        data_dict = request.json
        if not data_dict:
            data_dict = request.args
        if not data_dict:
            data_dict = request.data
        if not data_dict:
            raise ValueError("参数不能为空")
        module_id = int(data_dict.get("module_id"))
        if not module_id:
            raise ValueError("module_id参数不能为空")
        transforms = TransformModel.query.filter_by(module_id=module_id).all()
        transform_app = []
        for transform_ in transforms:
            transform_app.append(transform_.to_dict())
        return jsonify({"status": 1, "data": transform_app})
    except ValueError as v:
        traceback.print_exc()
        return jsonify({"status": 0, "data": v.__str__()})
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": 0, "data": e.__str__()})


@api.route("/loader", methods=["get"])  # 查询所有的数据输出方法
def loader():
    try:
        loaders = LoaderModel.query.all()
        loaders_app = []
        for load in loaders:
            loaders_app.append(load.to_dict())
        return jsonify({"status": 1, "data": loaders_app})
    except Exception as e:
        return jsonify({"status": 0, "data": e.__str__()})


@csrf.exempt
@api.route("/task", methods=["POST", "GET"])  # 任务提交接口
def task():
    if request.method == "POST":
        try:
            dict_data = request.get_json()
            print(dict_data)
            # 校验参数
            if not dict_data.get("source").get("type"):
                raise Exception("缺少type参数")
            if int(dict_data.get("source").get("type")) != 3:  # 不是Excel方式
                if not dict_data.get("source").get("type") or not dict_data.get("target").get("type") or not \
                        dict_data.get("source").get("sql") or not dict_data.get("name") or not dict_data.get("target").get("table") \
                        or not dict_data.get("primary_key"):
                    raise ValueError("参数不完整")
                judge_empty.Judge_Empty(2, dict_data.get("source").get("connect"))
                judge_empty.Judge_Empty(2, dict_data.get("target").get("connect"))
                judge_empty.Judge_Empty(2, dict_data.get("primary_key"))
            else:
                if not dict_data.get("source").get("file_path") or not dict_data.get("target").get("table") or not \
                        dict_data.get("primary_key") or not dict_data.get("name"):
                    raise Exception("参数不完整")

            instance = TaskModel.query.filter_by(name=dict_data.get("name")).first()
            if not instance:
                task = TaskModel(name=str(dict_data.get("name")), source=str(dict_data["source"]), methods=str(dict_data["methods"]),
                                 target=str(dict_data["target"]),primary_key=str(dict_data["primary_key"]))
                db.session.add(task)
            else:
                TaskModel.query.filter_by(name=str(dict_data.get("name"))).update({"source": str(dict_data["source"]),
                                                                              "methods": str(dict_data["methods"]),
                                                                              "target": str(dict_data["target"]),"primary_key":str(dict_data["primary_key"])})
            db.session.commit()
            return jsonify({"status": 1, "data": "任务创建成功"})
        except ValueError as v:
            db.session.rollback()
            traceback.print_exc()
            return jsonify({"status": 0, "data": v.__str__()})
        except Exception as e:
            traceback.print_exc()
            db.session.rollback()
            return jsonify({"status": 0, "data": e.__str__()})

    if request.method == "GET":
        try:
            tasks = TaskModel.query.all()
            task_app = []
            for task in tasks:
                task_app.append(task.to_dict())
            return jsonify({"status": 1, "data": task_app})
        except Exception as e:
            return jsonify({"status": 0, "data": e.__str__()})


@csrf.exempt
@api.route("/task/scheduler", methods=["POST", "PUT", "GET"])  # 任务计划提交接口
def task_scheduler():
    if request.method == "POST":  # 创建任务计划
        try:
            dict_data = request.json
            if not dict_data.get("task_id") or not dict_data.get("schedule") or not dict_data.get("type") or not dict_data.get("update"):
                raise ValueError("参数不完整,缺少task_id 或者 schedule 或者type")
            task_id = int(dict_data.get("task_id"))
            internal = int(dict_data["schedule"])
            type_to = int(dict_data["type"])
            update = int(dict_data["update"])
            # 提取任务处理模块
            base_task = TaskModel()
            task = base_task.query.filter_by(id=task_id).first()
            if task is None:
                raise ValueError("该任务不存在")
            # 生成随机的任务id
            random_id = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M%S") + "%00d" % (
                random.randint(000, 999))
            task_schedule = TaskScheduleModel()
            schedule = task_schedule.query.filter_by(TaskID=random_id).first()
            if schedule:
                raise ValueError("随机TaskID已存在,请重新提交")
            new_task_scheduler = TaskScheduleModel(task_name=task.name,task_id=task_id,TaskID=random_id,update=update,schedule=internal,status=0,type=type_to)
            db.session.add(new_task_scheduler)
            db.session.commit()
            return jsonify({"status": 1, "data": "任务计划创建成功"})
        except ValueError as v:
            traceback.print_exc()
            db.session.rollback()
            return jsonify({"status": 0, "data": v.__str__()})
        except Exception as e:
            traceback.print_exc()
            db.session.rollback()
            return jsonify({"status": 0, "data": e.__str__()})
    if request.method == "PUT":  # 启动任务计划
        try:
            # data_dict = request.get_json()
            data_dict = request.form
            if not data_dict:
                raise ValueError("参数不能为空")
            if not data_dict.get("task_id"):
                raise ValueError("缺少id或status参数")
            task_id = int(data_dict["task_id"])
            status = int(data_dict["status"])
            # status = int(data_dict["status"])
            task_schedule = TaskScheduleModel()
            schedule = task_schedule.query.filter_by(TaskID=task_id).first()
            if not schedule:
                raise ValueError("该任务不存在")
            schedule.status = status
            db.session.commit()
            return jsonify({"status": 1, "data": "任务操作成功,请查看任务执行日志"})
        except ValueError as v:
            traceback.print_exc()
            db.session.rollback()
            return jsonify({"status": 0, "data": v.__str__()})
        except Exception as e:
            traceback.print_exc()
            db.session.rollback()
            return jsonify({"status": 0, "data": e.__str__()})
    if request.method == "GET":  # 查询任务计划
        try:
            task_schedule = TaskScheduleModel()
            schedules = task_schedule.query.all()
            data_app = []
            for schedule in schedules:
                data_app.append(schedule.to_dict())
            return jsonify({"status": 1, "data": data_app})
        except ValueError as v:
            traceback.print_exc()
            return jsonify({"status": 0, "data": v.__str__()})
        except Exception as e:
            traceback.print_exc()
            return jsonify({"status": 0, "data": e.__str__()})