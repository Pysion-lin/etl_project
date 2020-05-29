from ETLSchedule.ETL.transformer.trannform import BaseTransForm
from .GetFunctionArgs import get_func_args
from ETLSchedule.models import session
from ETLSchedule.models.models import TransformModel
import traceback


def update_resource():
    try:
        transform = BaseTransForm()
        for name in transform.methods__():
            args = get_func_args(name, transform)
            instance = session.query(TransformModel).filter_by(name=name).first()
            tmp_func = getattr(transform, name)
            if not instance:
                function_model = TransformModel(name=name, args=str(args), description=tmp_func.__doc__)
                session.add(function_model)
            else:
                session.query(TransformModel).filter_by(name=name).update({"args": str(args), "description": tmp_func.__doc__})
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            traceback.print_exc()  # TODO 启动日志记录
    except Exception as e:
        traceback.print_exc()  # TODO 启动日志记录
