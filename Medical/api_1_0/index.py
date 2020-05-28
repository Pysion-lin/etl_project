from . import api
from Medical.models import User
# import logging
from flask import current_app,jsonify


@api.route("/index")
def index():
    user_infos = User.query.filter(User.EmployeeID>1).all()
    user_info_app = []
    for user_info in user_infos:
        user_info_app.append(user_info.to_dict())
        # print(user_info.to_dict())
    return jsonify({"status":1,"data":user_info_app})