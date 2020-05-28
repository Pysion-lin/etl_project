from Medical.etl.extracter.extract import Extract
from Medical.etl.loader.loader import LoadData
from Medical.etl.transformer import BaseTransForm
from . import api
from Medical import db
from flask import current_app,jsonify,g
from Medical.models import ExtractModel,TransformModel,LoaderModel
import pickle,inspect


def get_func_args(name,obj):
    method = getattr(obj, name)
    return [x for x in inspect.getargspec(method).args if x != "self" and x != "dataframe"]


@api.route("/initialize")
def initialize():
    try:
        extract = Extract()
        transform = BaseTransForm()
        loader = LoadData()
        for name in extract.methods__():
            args = get_func_args(name,extract)
            instance = ExtractModel.query.filter_by(name=name).first()
            tmp_func = getattr(extract,name)
            if not instance:
                function_model = ExtractModel(name=name,args=str(args),description=tmp_func.__doc__)
                db.session.add(function_model)
            else:
                ExtractModel.query.filter_by(name=name).update({"args":str(args),"description":tmp_func.__doc__})

        for name in transform.methods__():
            args = get_func_args(name, transform)
            instance = TransformModel.query.filter_by(name=name).first()
            tmp_func = getattr(transform, name)
            if not instance:
                function_model = TransformModel(name=name,args=str(args),description=tmp_func.__doc__)
                db.session.add(function_model)
            else:
                TransformModel.query.filter_by(name=name).update({"args":str(args),"description":tmp_func.__doc__})

        for name in loader.methods__():
            args = get_func_args(name, loader)
            instance = LoaderModel.query.filter_by(name=name).first()
            tmp_func = getattr(loader, name)
            if not instance:
                function_model = LoaderModel(name=name,args=str(args),description=tmp_func.__doc__)
                db.session.add(function_model)
            else:
                LoaderModel.query.filter_by(name=name).update({"args":str(args),"description":tmp_func.__doc__})
        try:
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return jsonify({"status": 1, "data": e.__str__()})
        scheduler = current_app.config.get("SCHEDULER")
        if not scheduler.scheduler.running:
            scheduler.start()
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": 1, "data": e.__str__()})
    return jsonify({"status":1,"data":"ok"})
