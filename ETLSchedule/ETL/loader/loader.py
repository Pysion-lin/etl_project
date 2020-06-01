from ETLSchedule.models import engine, MetaData, Table
from ETLSchedule.models.models import EmployeeModel
import pandas as pd
import datetime, time


class LoadData(object):
    def __init__(self):
        # print("loadData __init__")
        pass

    # @staticmethod
    # def save_data(x):
    # print("x", x)
    # log_obj = HttpLogModel(domain=x[0], method=x[1], url=x[2], code=x[3])
    # session.add(log_obj)
    # session.commit()
    # with open("save_data","w") as f:
    #     f.write(x)
    # print("save data finish")
    # return True
    def save_data_to_mysql(self, dataframe, tb="tb_bdm_employee"):
        '''将dataframe的数据装载到mysql数据库中,tb是表名'''
        dataframe.to_sql(tb, engine, if_exists='replace', index=False)

    # def load_data(self, dataframe):
    #     print("dataframe:", dataframe[1])
    #     # data_result = dataframe.map(self.save_data)
    #     # data_result = dataframe.applay(self.save_data)
    #     print("start load data")

    def insert_mysql_data(self, engine, table_name, data_dict):
        # sql_comment = "insert into  {table_name} {filed_list}  values {data_tuple}".format(
        #                 table_name=table_name,filed_list=filed_list, data_tuple=data_tuple)
        # sql_comment = "insert into tb_bdm_employee 'EmployeeID'  values 5"
        # # print(sql_comment)
        # cursor = session.execute(sql_comment)
        # session.commit()
        # result = cursor.lastrowid
        # print(cursor.lastrowid)

        # 绑定引擎
        metadata = MetaData(engine)

        # 连接数据表
        tb_bdm_employee = Table(table_name, metadata, autoload=True)
        # address_table = Table('address', metadata, autoload=True)

        # 连接引擎
        conn = engine.connect()

        ins = tb_bdm_employee.insert()

        # 传递参数并执行语句
        result = conn.execute(ins, **data_dict)

        # # 执行多条语句
        # result = conn.execute(tb_bdm_employee.insert(), [data_dict])

        return result

    def select_mysql_data(self, session, sql_comment):
        cursor = session.execute(sql_comment)
        result = cursor.fetchall()
        return result

    def update_mysql_data(self, session, table_name, data_dict):
        sql_comment = 'update {table_name} set  {};'.format(table_name, *data_dict)
        # print(sql_comment)
        cursor = session.execute(sql_comment)
        session.commit()
        result = cursor.lastrowid
        # print(cursor.lastrowid)
        return result

    def sql_to_mysql(self, df, target, primary_key):
        extract = Extract()
        session = extract.create_mysql_session__(target["connect"])
        engine = extract.create_mysql_engin__(target["connect"])
        if target.get("type") == 1:  # mysql方式
            for index, row in df.iterrows():
                for name in list(df.keys()):
                    # print({name: row[name]})
                    if name == primary_key.get("to_primary_field"):
                        # session.query(EmployeeModel).filter_by(f=row[name]).first()
                        sql = "select {filed} from {table} where {filed} = '{value}'".format(
                            filed=primary_key.get("to_primary_field"),
                            table=target.get("table"),
                            value=str(row[name]))
                        ret = self.select_mysql_data(session, sql)
                        if not ret:
                            data_dict = dict(row.items())
                            for key, value in data_dict.items():
                                if value is not None:
                                    if type(value) == pd.Timestamp:
                                        data_dict[key] = datetime.datetime.strftime(data_dict[key],"%Y-%m-%d %H:%M:%S")
                                if type(data_dict[key]) == type(pd.NaT):
                                    data_dict[key] = None
                                # if key == "Version":
                                #     print("value:",value,type(value))
                                #     print("values_",datetime.datetime.timestamp(bytes(value).decode()))
                            data_dict["Version"] = None

                            ret = self.insert_mysql_data(engine, target.get("table"), data_dict)

    def methods__(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))


if __name__ == '__main__':
    from ETLSchedule.ETL.extracter.extract import Extract

    a = "{'type': 2, 'connect': {'database': 'CRM', 'ip': '192.168.1.100\\sql2008', 'password': 'test', 'user': 'test'}, 'table': 'TB_BDM_Employee', 'sql': 'select top 1 * from TB_BDM_Employee'}"
    source_connect = dict({'database': 'CRM', 'ip': '192.168.1.100\\sql2008', 'password': 'test', 'user': 'test'})
    b = {'type': 1,
         'connect': {'database': 'pyetl', 'ip': '192.168.11.31', 'password': '12345678', 'port': 3306, 'user': 'root'},
         'table': 'tb_bdm_employee', 'sql': 'select * from tb_bdm_employee limit 1'}
    target = dict(b)
    primary_key = {"from_primary_filed": "EmployeeName", "to_primary_field": "EmployeeName"}
    extract = Extract()
    loader = LoadData()
    data_frame = extract.read_sqlserver("select top 10 * from TB_BDM_Employee", source_connect)
    # data_frame["Version"] = data_frame["Version"].map(lambda x: 1 if x is not None else 0)
    # print(data_frame.items())
    # loader.save_data_to_mysql(data_frame, tb="tb_bdm_employee")
    loader.sql_to_mysql(data_frame, target, primary_key)
