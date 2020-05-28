from Medical.etl.extracter.extract import Extract
from . import api
from Medical import db,csrf
from flask import current_app,jsonify,request
from Medical.models import ExtractModel
import pickle


@csrf.exempt
@api.route("/uninitialize",methods=["POST"])
def uninitialize():
    try:
        function_data = request.get_json()
        if not function_data.get("id"):
            raise ValueError("参数不完整")
        extract = Extract()
        id = int(function_data.get("id"))
        file_path = function_data.get("file_path")
        instance = ExtractModel.query.filter_by(id=id).first()
        if not instance:
            raise ValueError("该方法不存在")
        # func = instance.function_serializer
        func_name = instance.function_name
        func_ = getattr(extract,func_name)
        if not func_:
            raise ValueError("该方法未定义")
        result = func_(file_path)
    except Exception as e:
        return jsonify({"status": 0, "data": e.__str__()})
    return jsonify({"status":1,"data":"提交成功"})