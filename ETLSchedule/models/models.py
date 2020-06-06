from sqlalchemy import Column
from sqlalchemy.orm import validates
from sqlalchemy.types import String, Integer,Date,Binary,TIMESTAMP,Boolean
from sqlalchemy.ext.declarative import declarative_base


BaseModel = declarative_base()  # 创建对象的基类


class HttpLogModel(BaseModel):
    __tablename__ = 'log_info'  # 创建表，指定表名称
    # 指定表的结构
    id = Column(Integer, primary_key=True)
    domain = Column(String, index=True)
    method = Column(String, index=True)
    url = Column(String, index=True)
    code = Column(String, index=True)

    @validates('code')
    def validate_code(self, key, value):
        assert value != ''
        return value


class TaskModel(BaseModel):
    __tablename__ = "tb_task"
    id = Column(Integer,primary_key=True)
    name = Column(String)
    source = Column(String)
    methods = Column(String)
    target = Column(String)
    primary_key = Column(String)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class TaskScheduleModel(BaseModel):
    __tablename__ = "tb_task_schedule"
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer)
    TaskID = Column(String)
    task_name = Column(String)
    schedule = Column(String)
    status = Column(Integer)
    logs = Column(String)


class TBEmployeeModel(BaseModel):
    __tablename__ = 'TB_BDM_Employee'  # 创建表，指定表名称
    # 指定表的结构
    EmployeeID = Column(Integer, primary_key=True)
    EmployeeCode = Column(String, index=True)
    EmployeeName = Column(String, index=True)
    BirthDate = Column(Date, index=True)
    HireDate = Column(Date, index=True)
    StateID = Column(String, index=True)
    AlphabetCode = Column(String, index=True)
    Sex = Column(String, index=True)
    PositionID = Column(String, index=True)
    Address = Column(String, index=True)
    Tel = Column(String, index=True)
    EMail = Column(String, index=True)
    Notes = Column(String, index=True)
    PostalCode = Column(String, index=True)
    DeptCode = Column(String, index=True)
    Version = Column(TIMESTAMP, index=True)
    IsChecker = Column(String, index=True)
    IsDownLoad = Column(String, index=True)
    HospitalCode = Column(String, index=True)

    # 把SQLAlchemy查询对象转换成字典
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class EmployeeModel(BaseModel):
    __tablename__ = 'tb_bdm_employee'  # 创建表，指定表名称
    # 指定表的结构
    id = Column(Integer, primary_key=True)
    EmployeeID = Column(Integer)
    EmployeeCode = Column(String, index=True)
    EmployeeName = Column(String, index=True)
    BirthDate = Column(Date, index=True)
    HireDate = Column(Date, index=True)
    StateID = Column(String, index=True)
    AlphabetCode = Column(String, index=True)
    Sex = Column(String, index=True)
    PositionID = Column(String, index=True)
    Address = Column(String, index=True)
    Tel = Column(String, index=True)
    EMail = Column(String, index=True)
    Notes = Column(String, index=True)
    PostalCode = Column(String, index=True)
    DeptCode = Column(String, index=True)
    Version = Column(TIMESTAMP, index=True)
    IsChecker = Column(String, index=True)
    IsDownLoad = Column(String, index=True)
    HospitalCode = Column(String, index=True)

    # @validates('code')
    # def validate_code(self, key, value):
    #     assert value != ''
    #     return value

    # 把SQLAlchemy查询对象转换成字典
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


# 数据源类型
class ResourceType(BaseModel):
    __tablename__ = "tb_resource_type"
    id = Column(Integer, primary_key=True)
    type = Column(String)
    description = Column(String)


# 功能模块的模型类
class TransformModel(BaseModel):
    '''功能函数'''
    __tablename__ = "tb_transform"
    id = Column(Integer,primary_key=True)
    name = Column(String)
    args = Column(String)
    is_primary_key = Column(Boolean)
    description = Column(String)
    module_id = Column(Integer)

    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


