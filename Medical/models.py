from . import db


class User(db.Model):
    """用户"""

    __tablename__ = "tb_bdm_employee"
    EmployeeID = db.Column(db.Integer, primary_key=True)
    EmployeeCode = db.Column(db.String, index=True)
    EmployeeName = db.Column(db.String, index=True)
    BirthDate = db.Column(db.Date, index=True)
    HireDate = db.Column(db.Date, index=True)
    StateID = db.Column(db.String, index=True)
    AlphabetCode = db.Column(db.String, index=True)
    Sex = db.Column(db.String, index=True)
    PositionID = db.Column(db.String, index=True)
    Address = db.Column(db.String, index=True)
    Tel = db.Column(db.String, index=True)
    EMail = db.Column(db.String, index=True)
    Notes = db.Column(db.String, index=True)
    PostalCode = db.Column(db.String, index=True)
    DeptCode = db.Column(db.String, index=True)
    Version = db.Column(db.String, index=True)
    IsChecker = db.Column(db.String, index=True)
    IsDownLoad = db.Column(db.String, index=True)
    HospitalCode = db.Column(db.String, index=True)

    # 把SQLAlchemy查询对象转换成字典
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    # @property
    # def password(self):
    #     """对应password属性的读取操作"""
    #     raise AttributeError("不支持读取操作")
    #
    # @password.setter
    # def password(self, value):
    #     """对应password属性的设置操作, value用户设置的密码值"""
    #     self.password_hash = generate_password_hash(value)
    #
    # def check_password(self, value):
    #     """检查用户密码， value 是用户填写密码"""
    #     return check_password_hash(self.password_hash, value)
    #
    # def to_dict(self):
    #     """将对象转换为字典数据"""
    #     user_dict = {
    #         "user_id": self.id,
    #         "name": self.name,
    #         "mobile": self.mobile,
    #         "avatar": constants.QINIU_URL_DOMAIN + self.avatar_url if self.avatar_url else "",
    #         "create_time": self.create_time.strftime("%Y-%m-%d %H:%M:%S")
    #     }
    #     return user_dict
    #
    # def auth_to_dict(self):
    #     """将实名信息转换为字典数据"""
    #     auth_dict = {
    #         "user_id": self.id,
    #         "real_name": self.real_name,
    #         "id_card": self.id_card
    #     }
    #     return auth_dict


class ExtractModel(db.Model):
    '''功能函数'''
    __tablename__ = "tb_extracter"
    id = db.Column(db.Integer,primary_key=True)
    name = db.Column(db.String)
    # serializer_no = db.Column(db.Binary)
    args = db.Column(db.String)
    description = db.Column(db.String)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class TransformModel(db.Model):
    '''功能函数'''
    __tablename__ = "tb_transform"
    id = db.Column(db.Integer,primary_key=True)
    name = db.Column(db.String)
    module_id = db.Column(db.Integer)
    args = db.Column(db.String)
    description = db.Column(db.String)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class TransformModuleModel(db.Model):
    __tablename__ = "tb_transform_module"
    id = db.Column(db.Integer,primary_key=True)
    name = db.Column(db.String)
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class LoaderModel(db.Model):
    '''功能函数'''
    __tablename__ = "tb_loader"
    id = db.Column(db.Integer,primary_key=True)
    name = db.Column(db.String)
    # serializer_no = db.Column(db.Binary)
    args = db.Column(db.String)
    description = db.Column(db.String)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class TaskModel(db.Model):
    __tablename__ = "tb_task"
    id = db.Column(db.Integer,primary_key=True)
    name = db.Column(db.String)
    source = db.Column(db.String)
    methods = db.Column(db.String)
    target = db.Column(db.String)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# 数据源类型
class ResourceType(db.Model):
    __tablename__ = "tb_resource_type"
    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String)
    description = db.Column(db.String)
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}