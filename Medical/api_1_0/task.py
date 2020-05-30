from . import api
from flask import current_app, jsonify, request
from Medical import csrf, db
from Medical.models import ExtractModel, TransformModel, LoaderModel, TaskModel, ResourceType,TransformModuleModel
import traceback, random, datetime
from Medical.api_1_0.utils.connect import TestConnect
from Medical.api_1_0.utils import judge_empty
from Medical.api_1_0.utils import parse_parameter


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

            type_id = dict_data.get("id")
            if not type_id:
                raise ValueError("类型id不存在")
            # 校验参数
            if not all([dict_data.get("description"), dict_data["id"], dict_data.get("type"), dict_data.get("sql"),
                        dict_data.get("table"), dict_data["description"].get("database"),
                        dict_data["description"].get("ip"), dict_data["description"].get("password"),
                        dict_data["description"].get("user")]):
                raise ValueError("参数不完整")
            connect = TestConnect()
            if int(type_id) == 1:
                if not all([dict_data["description"].get("port")]):
                    raise ValueError("参数不完整,缺少port参数")
                connect.mysql_connect_test(dict_data.get("description"))
            elif int(type_id) == 2:
                connect.sqlserver_connect_test(dict_data.get("description"))
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
        type_id = dict_data.get("id")  # mysql/SQLserver
        type_name = dict_data.get("type")  # source/target
        if not type_id:
            raise ValueError("类型id不存在")
        if not type_name:
            raise ValueError("类型名称不存在")
        # 校验参数
        judge_empty.Judge_Empty(2, dict_data.get("connect"))
        connect = TestConnect()

        if request.method == "GET":  # 获取字段
            ret = None
            if type_name == "source":  # 类型为源的字段
                sql = dict_data.get("sql")
                if not sql:
                    raise ValueError("sql不存在")
                sql = sql + " "
                if int(type_id) == 1:  # 获取连接为mysql的字段
                    connect.mysql_connect_test(dict_data.get("connect"))  # 测试是否连通
                    parse_sql = parse_parameter.get_str_btw(sql,"select","from")
                    print("parse_sql",parse_sql,type(parse_sql))
                    if "*" in parse_sql:
                        table = connect.get_table(sql)
                        ret = connect.get_mysql_field_from_engin(table)
                    else:
                        ret = parse_sql
                elif int(type_id) == 2: # 获取连接为SQLserver的字段
                    connect.sqlserver_connect_test(dict_data.get("connect"))
                    parse_sql = parse_parameter.get_str_btw1(sql, "select", "from")
                    print(type(parse_sql),parse_sql)
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
                if int(type_id) == 1:  # 获取连接为mysql的字段
                    connect.mysql_connect_test(dict_data.get("connect"))  # 测试是否连通
                    ret = connect.get_mysql_field_from_engin(table)
                elif int(type_id) == 2: # 获取连接为SQLserver的字段
                    connect.sqlserver_connect_test(dict_data.get("connect"))
                    ret = connect.sqlserver_get_table_field(table)
            return jsonify({"status": 1, "data": ret})
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
            raise ValueError("参数不能为空")
        module_id = data_dict.get("module_id")
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
            if not dict_data.get("source").get("type") or not dict_data.get("target").get("type") or \
                    not dict_data.get("source").get("table") or not dict_data.get("source").get("sql") or \
                    not dict_data.get("target").get("table") or not dict_data.get("target").get("sql") or not dict_data.get("name"):
                raise ValueError("参数不完整")
            judge_empty.Judge_Empty(2, dict_data.get("source").get("connect"))
            judge_empty.Judge_Empty(2, dict_data.get("target").get("connect"))
            for methods in dict_data.get("methods"):
                judge_empty.Judge_Empty(2, methods.get("args"))

            instance = TaskModel.query.filter_by(name=dict_data.get("name")).first()
            if not instance:
                task = TaskModel(name=str(dict_data.get("name")), source=str(dict_data["source"]), methods=str(dict_data["methods"]),
                                 target=str(dict_data["target"]))
                db.session.add(task)
            else:
                TaskModel.query.filter_by(name=str(dict_data.get("name"))).update({"source": str(dict_data["source"]),
                                                                              "methods": str(dict_data["methods"]),
                                                                              "target": str(dict_data["target"])})
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
@api.route("/task/scheduler", methods=["POST", "GET"])  # 任务提交接口
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
            extracter_func = getattr(base_task, extract_name)
            ## 提取transform模块
            transform_func = getattr(base_task, transform_name)
            ## 提取loader模块
            loader_func = getattr(base_task, loader_name)

            # 组装一个任务
            def new_task(_extracter_func, _transform_func, _loader_func, extract_parameter, transform_parameter):
                print("任务开始")
                1 / 0
                # dataframe = _extracter_func(**dict(eval(extract_parameter)))
                # transform_parameter = dict(eval(transform_parameter))
                # transform_parameter["dataframe"] = dataframe
                # _dataframe = _transform_func(**transform_parameter)
                # result = _loader_func(_dataframe)

            # 添加一个任务

            # 生成随机的任务id
            random_id = datetime.datetime.strftime(datetime.datetime.now(), "%Y%m%d%H%M%S") + "%00d" % (
                random.randint(000, 999))
            try:
                scheduler = current_app.config.get("SCHEDULER")
                scheduler.add_job(new_task, trigger="interval", seconds=int(task.scheduler),
                                  data=[extracter_func, transform_func, loader_func, task.extract_parameter,
                                        task.transform_parameter], job_id=random_id)
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
        return jsonify({"status": 0, "data": e.__str__()})
