from ETLSchedule.models import engine, MetaData, Table
from ETLSchedule.models.models import EmployeeModel
import pandas as pd
import numpy as np
import datetime, time
import traceback
import os,sys


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

    def insert_mysql_data(self, engine, table_name, data_dict, logger):
        # sql_comment = "insert into  {table_name} {filed_list}  values {data_tuple}".format(
        #                 table_name=table_name,filed_list=filed_list, data_tuple=data_tuple)
        # sql_comment = "insert into tb_bdm_employee 'EmployeeID'  values 5"
        # # print(sql_comment)
        # cursor = session.execute(sql_comment)
        # session.commit()
        # result = cursor.lastrowid
        # print(cursor.lastrowid)
        try:
            # 绑定引擎
            metadata = MetaData(engine)

            # 连接数据表
            tb_bdm_employee = Table(table_name, metadata, autoload=True)
            # address_table = Table('address', metadata, autoload=True)

            # 连接引擎
            conn = engine.connect()

            ins = tb_bdm_employee.insert()

            # 传递参数并执行语句
            # print('insert_data_dict', data_dict)
            # conn.transaction()
            result = conn.execute(ins, **data_dict)

            # # 执行多条语句
            # result = conn.execute(tb_bdm_employee.insert(), [data_dict])

            return result.lastrowid

        except Exception as e:
            traceback.print_exc()
            file_path = os.path.join(sys.path[0], "log",
                                     "%s" % datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
            log = logger(filename=file_path)
            log.removeHandler(log.handlers)
            log.info(e.__str__())
            traceback.print_exc()
            return None

    def multi_insert_mysql_data(self, engine, data_dict, logger):
        # print('new_dict',new_dict)
        # 连接引擎
        # conn = engine.connect()
        # 传递参数并执行语句
        try:
            # 绑定引擎
            metadata = MetaData(engine)
            # 连接数据表
            # print("all_insert_data_dict:",data_dict)
            new_dict = {}
            for data_list in data_dict:
                for table, data in data_list.items():
                    tb_bdm_employee = Table(table, metadata, autoload=True)
                    ins = tb_bdm_employee.insert()
                    new_dict[ins] = data
            # print('new_dict', new_dict)
            with engine.begin() as conn:
                # conn = engine.begin()
                for ins, data in new_dict.items():
                    result = conn.execute(ins, **data)
            return result.lastrowid
        except Exception as e:
            file_path = os.path.join(sys.path[0], "log",
                                     "%s" % datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
            log = logger(filename=file_path)
            log.removeHandler(log.handlers)
            log.info(e.__str__())
            traceback.print_exc()
            return False

    def select_mysql_data(self, session, sql_comment):
        cursor = session.execute(sql_comment)
        result = cursor.fetchall()
        return result

    def update_mysql_data(self, engine, schema, table_name, data_dict, where, logger):
        # sql_comment = 'UPDATE %s SET ' % table_name + ','.join(['%s=%r' % (k, data_dict[k]) for k in data_dict]) + ' WHERE %s=%r;' % (where[0], where[1])
        # cursor = session.execute(sql_comment)
        # session.commit()
        # result = cursor.lastrowid
        # print("cursor",cursor)
        # print("result",result)
        # print(cursor.lastrowid)
        try:
            # 绑定引擎
            metadata = MetaData(engine)
            # 连接数据表
            tb_bdm_employee = Table(table_name, metadata, autoload=True)
            # address_table = Table('address', metadata, autoload=True)
            # 连接引擎
            conn = engine.connect()
            # print('where', where)
            # print('data_dict', data_dict)
            ins = tb_bdm_employee.update().where(schema.Column(where[0]) == where[1]).values(
                **data_dict)  # table.update().where(table.c.id==7).values(name='foo')
            # 传递参数并执行语句
            result = conn.execute(ins)
            return result
        except Exception as e:
            file_path = os.path.join(sys.path[0], "log",
                                     "%s" % datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))
            log = logger(filename=file_path)
            log.removeHandler(log.handlers)
            log.info(e.__str__())
            traceback.print_exc()
            return None

        # return result

    def sql_to_mysql(self, df, target, primary_key, extract, schema, logger):
        from pandas.api.types import is_datetime64_any_dtype
        session = extract.create_mysql_session__(target["connect"])
        engine = extract.create_mysql_engin__(target["connect"])
        if target.get("type") == 1:  # mysql方式
            for index, row in df.iterrows():
                name = primary_key.get("to_primary_field")
                if name in list(df.keys()):
                    sql = "select {filed} from {table} where {filed} = '{value}'".format(
                        filed=name,
                        table=target.get("table"),
                        value=str(row[name]))
                    ret = self.select_mysql_data(session, sql)  # 查询该行的值是否是主键列并且判断是否存在
                    # row = row.where(row.,None)
                    data_dict = dict(row.items())
                    # print('data_dict1',data_dict)
                    for key, value in data_dict.items():
                        if type(value) == pd.Timestamp:
                            if value:
                                data_dict[key] = datetime.datetime.strftime(data_dict[key], "%Y-%m-%d %H:%M:%S")
                            else:
                                data_dict[key] = None
                        if type(value) == type(pd.NaT):
                            # print('NaT',data_dict[key],key)
                            data_dict[key] = None
                    if not ret:  # 如果结果不存在,将数据插入
                        print("row[name]", row[name])
                        self.insert_mysql_data(engine, target.get("table"), data_dict, logger)
                    else:  # 如果结果存在,将数据更新
                        where = (name, data_dict[name])
                        # 剔除主键的字段,不然会报错
                        del data_dict[name]
                        if not data_dict:
                            raise Exception("任务可能由于没有目标列报错,请检查任务!")
                        self.update_mysql_data(engine, schema, target.get("table"), data_dict, where, logger)
                else:
                    raise ValueError("主键:{} 不存在SQL语句中".format(name))
        else:
            raise Exception("目前只支持mysql目标库")

    def serial_sql_to_mysql(self, df, target, primary_key, extract, schema, logger):
        session = extract.create_mysql_session__(target["connect"])
        if target.get("type") == 1:  # mysql方式
            for index, row in df.iteritems():
                name = primary_key.get("to_primary_field")
                if name in list(df.keys()):
                    sql = "select {filed} from {table} where {filed} = '{value}'".format(
                        filed=name,
                        table=target.get("table"),
                        value=str(row[name]))
                    ret = self.select_mysql_data(session, sql)  # 查询该行的值是否是主键列并且判断是否存在
                    data_dict = dict(row.items())
                    for key, value in data_dict.items():
                        if value is not None:
                            if type(value) == pd.Timestamp:
                                data_dict[key] = datetime.datetime.strftime(data_dict[key], "%Y-%m-%d %H:%M:%S")
                        if type(data_dict[key]) == type(pd.NaT):
                            data_dict[key] = None
                    if data_dict.get("Version"):
                        data_dict["Version"] = None
                    if not ret:  # 如果结果不存在,将数据插入
                        self.insert_mysql_data(engine, target.get("table"), data_dict, logger)
                    else:  # 如果结果存在,将数据更新
                        where = (name, data_dict[name])
                        # 剔除主键的字段,不然会报错
                        del data_dict[name]
                        if not data_dict:
                            raise Exception("任务可能由于没有目标列报错,请检查任务!")
                        self.update_mysql_data(engine, schema, target.get("table"), data_dict, where, logger)
                else:
                    raise ValueError("主键:{} 不存在SQL语句中".format(name))

    def sql_to_record_mysql(self, df, target, primary_key, extract, schema, logger):
        session = extract.create_mysql_session__(target["connect"])
        engine = extract.create_mysql_engin__(target["connect"])
        try:
            pd.set_option('display.max_rows', None)
            # print('personal_info_df',df[0])
            # print('result_info_df',df[1])
            # print('personal_info_columns',df[2])
            # print('result_info_columns',df[3])
            # print('tj_item_detail_df_1:',df[4])
            # print('tj_items_df:',df[5])
            # print('tj_items_df_:',df[5])
            personal_info_df = df[0]
            result_info_df = df[1]
            personal_info_field_list = df[2]
            tj_result_field_list = df[3]
            tj_item_detail_df_1 = df[4]
            tj_items_df = df[5]
            tj_record_df = df[6]

            personal_info_field_list.append("DABH")
            tj_result_field_list.append("DABH")
            if target.get("type") == 1:  # mysql方式
                for index, row in personal_info_df.iterrows():  # 遍历个人用户信息表
                    all_execute = []
                    data_dict = dict(row.items())
                    info_DABH = data_dict.get("DABH")
                    personal_info_dict = data_dict
                    tj_result_list = []  # 将对应ti_result的数据准备好
                    tj_items_list = []  # 将tj_items数据准备好
                    tj_item_detail_list = []  # 将tj_item_detail数据准备好
                    tj_record_list = []  # 将tj_record数据准备好
                    result_info_onece_df = result_info_df.query("DABH == '%s'" % info_DABH)
                    for index,result_info in result_info_onece_df.iterrows():  # result_info_df数据,通过DABH与personal_info_df匹配
                        da_dict = dict(result_info)
                        tj_result_list.append(da_dict)
                    tj_items_onece_df = tj_items_df.query("DABH == '%s'" % info_DABH)
                    for index,tj_items in tj_items_onece_df.iterrows():  # tj_items数据,通过DABH与personal_info_df匹配
                        da_dict = dict(tj_items)
                        tj_items_list.append(da_dict)
                        tj_items_id = da_dict.get("ID")
                        tj_item_detail_onece_df = tj_item_detail_df_1.query("TJ_ITEMID == %s" % tj_items_id)
                        for index_,tj_item_detail in tj_item_detail_onece_df.iterrows():  # tj_item_detail数据,通过tj_item_detail的TJ_ITEMID与tj_items_df的ID匹配
                            detail_dict = dict(tj_item_detail)
                            tj_item_detail_list.append(detail_dict)
                    tj_record_onece_df = tj_record_df.query("DABH == '%s'" % info_DABH)
                    for index,tj_record in tj_record_onece_df.iterrows():
                        tj_record_list.append(dict(tj_record))

                    # 更新wj_answer表的DABH
                    where = ("ID", personal_info_dict['OLD_ID'])  # 再transform时将OLD_ID添加到personal_info中,更新完wj_answer_master将其删除
                    self.update_mysql_data(engine, schema, "wj_answer_master", {"DABH": info_DABH}, where)
                    result = self.personal_de_weight_strategy(session,personal_info_field_list, personal_info_dict)
                    if not result:  # 如果结果不存在,将数据插入
                        print("INSERT:")
                        del personal_info_dict['OLD_ID']
                        all_execute.append({"personal_info": personal_info_dict})
                        for tj_record_dict in tj_record_list:
                            all_execute.append({"tj_record": tj_record_dict})
                        res = self.multi_insert_mysql_data(engine, all_execute, logger)
                        # print('res:',res)
                        if res:  # TODO 此处需要在处理当插入数据错误时,需要删除前面新建的个人信息和体检主表
                            for tj_result in tj_result_list:
                                tj_result["ID_O"] = res
                                ret = self.insert_mysql_data(engine, "tj_result", tj_result,logger)
                            for tj_item in tj_items_list:   # 将ti_item数据入库
                                tj_item["ID_O"] = res
                                # 更新test_apply的DABH
                                where = ("ID", tj_item['OLD_ID'])
                                self.update_mysql_data(engine, schema, "test_apply", {"DABH": tj_item.get("DABH")}, where,logger)
                                tj_item = self.change_NaT(tj_item)
                                ret = self.insert_mysql_data(engine, "tj_items", tj_item, logger)
                                if ret:   # 根据tj_item返回的主键ID将tj_item_detail入库
                                    for tj_item_detail in tj_item_detail_list:
                                        tj_item_detail["TJ_ITEMID"] = ret
                                        tj_item_detail = self.change_NaT(tj_item_detail)
                                        rest = self.insert_mysql_data(engine, "tj_item_detail", tj_item_detail, logger)
                        else:
                            raise Exception("数据插入错误")

                    else:  # 如果结果存在,将数据更新
                        print("UPDATE:")
                        # 更新个人信息表(通过档案编号更新)
                        where = ("DABH", info_DABH)
                        del personal_info_dict['OLD_ID']
                        self.update_mysql_data(engine, schema, "personal_info", personal_info_dict, where, logger)
                        # print('tmp2_table2',tmp2_table2)
                        tj_record_ret = self.select_mysql_data(session, "select * from %s where %s=%r limit 1" % ("tj_record", "DABH", info_DABH))  # 获取该条数据个人信息下的tj_record
                        # print("tj_record_ret",tj_record_ret)
                        if tj_record_ret:
                            # 更新tj_record 使用tj_record返回的ID
                            # print("len-tj_record_list", len(tj_record_list))
                            if len(tj_record_ret) > 1:
                                raise Exception("tj_record出现重复的DABH")
                            for tj_record_dict in tj_record_list:
                                where = ("ID_O", tj_record_ret[0][0])
                                tj_record_dict["ID_O"] = tj_record_ret[0][0]
                                ret = self.update_mysql_data(engine, schema, "tj_record", tj_record_dict, where, logger)
                            # 更新或者插入tj_result
                            tj_result_ret = self.select_mysql_data(session, "select * from %s where %s=%r" % (
                                "tj_result", "ID_O", tj_record_ret[0][0]))  # 获取该条数据个人信息下的tj_record的所有tj_result
                            if tj_result_ret:
                                tj_result_id_list = [tj_result[0] for tj_result in tj_result_ret]
                                tj_result_dict_code_list = []
                                for table in tj_result_list:  # 获取个人用户信息中所有来自问卷的体检DICT_CODE,目的是根据DICT_CODE过滤查询是否存在重复的tj_result
                                    tj_result_dict_code_list.append(int(table.get("DICT_CODE")))
                                parameter = ','.join(["%s"] * len(tj_result_id_list))
                                for tj_result_dict_code in tj_result_dict_code_list:
                                    sql = "select * from %s where %s in (%s) and %s = %r limit 1" % ("tj_result", "TJJLID", parameter,"DICT_CODE", tj_result_dict_code)
                                    sql = sql % tuple(tj_result_id_list)
                                    rest = self.select_mysql_data(session,sql)  # 查询tj_record下对应tj_result的所有id的DICT_CODE值是否已经存在,存在则更新,不存在则插入,目的是方便每次在映射表中添加映射关系是字段添加关联的tj_result数据
                                    tj_result = [table for table in tj_result_list if int(table.get("DICT_CODE")) == tj_result_dict_code][0]
                                    tj_result = self.change_NaT(tj_result)
                                    if "DABH" in tj_result.keys():
                                        del tj_result["DABH"]
                                    if rest:
                                        where = ("TJJLID",rest[0][0])
                                        ret = self.update_mysql_data(engine, schema, "tj_result", tj_result, where, logger)
                                    else:
                                        res = tj_record_ret[0][0]
                                        tj_result["ID_O"] = res
                                        self.insert_mysql_data(engine, "tj_result", tj_result, logger)
                            else:
                                for tj_result in tj_result_list:
                                    tj_result["ID_O"] = tj_record_ret[0][0]
                                    ret = self.insert_mysql_data(engine, "tj_result", tj_result, logger)

                            # 更新tj_items 使用tj_record返回的ID
                            for tj_item_dict in tj_items_list:
                                where = ("ID_O", tj_record_ret[0][0])
                                if tj_item_dict.get("OLD_ID"):
                                    del tj_item_dict["OLD_ID"]
                                tj_item_dict = self.change_NaT(tj_item_dict)
                                ret = self.update_mysql_data(engine, schema, "tj_items", tj_item_dict, where, logger)
                                if ret:
                                    # print("tj_item_detail_list",tj_item_detail_list)
                                    sql = "select * from tj_item_detail where %s = %r limit 1" % ("TJ_ITEMID",tj_item_dict["ID"])
                                    rest = self.select_mysql_data(session, sql)
                                    # 更新tj_items_detail 使用tj_items返回的ID
                                    if rest:  # 返回[(211, 1, '0029414', 1, None)]
                                        for tj_item_detail_dict in tj_item_detail_list:
                                            where = ("ID", rest[0][0])
                                            tj_item_detail_dict = self.change_NaT(tj_item_detail_dict)
                                            tj_item_detail_dict["ID"] = rest[0][0]
                                            ret = self.update_mysql_data(engine, schema, "tj_item_detail", tj_item_detail_dict, where, logger)

            else:
                raise Exception("目前只支持mysql目标库")
        except Exception as e:
            traceback.print_exc()
            session.rollback()
            print("xxxx")

    def change_NaT(self,data_dict):
        for key, value in data_dict.items():
            if value is not None:
                if type(value) == pd.Timestamp:
                    data_dict[key] = datetime.datetime.strftime(data_dict[key], "%Y-%m-%d %H:%M:%S")
            if type(data_dict[key]) == type(pd.NaT):
                data_dict[key] = None
        return data_dict

    def personal_de_weight_strategy(self, session, tj_result_field_list, field_dict):
        '''
        档案数据库的去重策略
        1,判断身份证号
        2,判断手机号+姓名
        3,判断
        '''
        # 身份证号:ID_CARD_NO
        ID_CARD_NO_field = ['ID_CARD_NO']  # 使用身份证号校验档案库是否已存在该数据
        allow_items = ["CITIZEN_NAME","GENDER","DABH"]
        other_field = [item for item in tj_result_field_list if item in allow_items]  # 使用姓名和性别校验档案库是否已存在该数据

        ID_CARD_NO_list = list(len(self.select_mysql_data(session, "select %s from %s where %s=%r limit 1" % (key, "personal_info", key, value))) > 0
                               for key, value in field_dict.items() if value and key in ID_CARD_NO_field)  # 将需要去重的字段添加到allow_field中并返回

        other_list = list(len(self.select_mysql_data(session,"select %s from %s where %s=%r limit 1" % (key,"personal_info",key,value))) > 0
                          for key,value in field_dict.items() if value and key in other_field)  # 将需要去重的字段添加到allow_field中并返回
        if len(list(filter(None,ID_CARD_NO_list))) > 0:
            return True
        if len(list(filter(None,other_list))) > 0:
            return True
        return False

    def redcord_database_filed(self):
        '''
        通过已存在的个人信息的档案编号,查询档案库是否存在对应的个人档案信息

        '''

    def methods__(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))


if __name__ == '__main__':
    from ETLSchedule.ETL.extracter.extract import Extract
    from ETLSchedule.ETL.transformer.trannform import BaseTransForm

    a = "{'type': 2, 'connect': {'database': 'CRM', 'ip': '192.168.1.100\\sql2008', 'password': 'test', 'user': 'test'}, 'table': 'TB_BDM_Employee', 'sql': 'select top 1 * from TB_BDM_Employee'}"
    source_connect = dict({'database': 'CRM', 'ip': '192.168.1.100\\sql2008', 'password': 'test', 'user': 'test'})
    b = {'type': 1,
         'connect': {'database': 'pyetl', 'ip': '192.168.11.31', 'password': '12345678', 'port': 3306, 'user': 'root'},
         'table': 'tb_bdm_employee', 'sql': 'select * from tb_bdm_employee limit 1'}
    target = dict(b)
    primary_key = {"from_primary_filed": "EmployeeName", "to_primary_field": "EmployeeName"}
    extract = Extract()
    loader = LoadData()
    transform = BaseTransForm()
    data_frame = extract.read_sqlserver("select top 10 * from TB_BDM_Employee", source_connect)
    # data_frame["Version"] = data_frame["Version"].map(lambda x: 1 if x is not None else 0)
    # print(data_frame.items())
    # loader.save_data_to_mysql(data_frame, tb="tb_bdm_employee")
    methods = [{'mapping': {'column': 'Sex', 'from_data': '男', 'to_data': '1'}},
               {'mapping': {'column': 'Sex', 'from_data': '女', 'to_data': '0'}}]
    df = data_frame
    for method in methods:
        for k, v in method.items():
            print(k, v)
            func = getattr(transform, k)
            print('func', func)
            df = func(df, **v)
            print('df:', df)
    loader.sql_to_mysql(df, target, primary_key)
