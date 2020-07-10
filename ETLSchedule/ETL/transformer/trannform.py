#
import pandas as pd
import datetime, random, traceback,os,sys
import numpy as np
from ETLSchedule.models import engine, MetaData, Table
from ETLSchedule import test_


class BaseTransForm(object):

    def __init__(self):
        '''输入列和输出列固定为column和to_column,select_type:固定为选中类型.这样方便在界面填写参数时的规范'''
        self.mapping_dict = {"module_id": 1, "primary_key": False}
        # self.split_data_dict = {"module_id":2,"primary_key":False}
        self.select_primary_key_dict = {"module_id": 3, "primary_key": True}
        self.transform_type_dict = {"module_id": 4, "primary_key": False}
        # self.virtual_field_dict = {"module_id":5,"primary_key":False}
        # self.none_transfrom_value_dict = {"module_id":4,"primary_key":False}
        # self.trans_position_dict = {"module_id":4,"primary_key":False}

    def mapping(self, dataframe, column, from_data, to_data, to_column):
        '''将某一列的值进行映射,column:列名,from_data:源数据,to_data:目标数据,to_column:目标列,如果from_data或者to_data为空则不做任何处理'''

        if from_data is None or to_data is None:
            return dataframe
        dataframe[column] = dataframe[column].map(lambda x: to_data if x == from_data else x)
        return dataframe

    def split_data(self, dataframe, column, sep, to_column):
        '''切分数据 column:字段名,sep:分割符,to_column:目标列'''
        dataframe[column] = dataframe[column].map(lambda x: str(x).split(sep)[0] if x is not None else x)
        return dataframe

    def select_primary_key(self, dataframe, column, to_column):
        '''选择字段作为装载数据时的判断是否重复的依据(每个任务必填),
        此处仅提供可视化选择对应的参数列表,实际处理在loader模块,column:源列名,to_column:目标列'''
        return dataframe

    def transform_type(self, dataframe, column, select_type):
        '''类型转换函数,column:要进行转换的列,select_type:要转换成的类型(请输入int,str),如果类型转换到目标库时失败任务将停止'''
        dataframe[column] = dataframe[column].map(lambda x: eval(select_type)(x))
        return dataframe

    def virtual_field(self, dataframe, virtual_column, to_column):
        '''虚拟字段,虚拟一个源字段并将该字段的数据写入到指定的目标列中,virtual_column:虚拟字段值,to_column:目标列'''
        dataframe["virtual_column"] = ''
        dataframe["virtual_column"] = dataframe["virtual_column"].map(lambda x: virtual_column)
        return dataframe

    def none_transfrom_value(self, dataframe, column, value):
        '''空值处理,column:要进行转换的列,value:将空值替换成的数值'''
        dataframe[column] = dataframe[column].map(lambda x: value
        if type(x) is pd._libs.tslibs.nattype.NaTType or x is None else x)
        return dataframe

    def trans_position(self, dataframe):
        '''将数据进行转置,将行数据转变成列数据,将列名转置为行数据'''
        dataframe = dataframe.stack()
        return dataframe

    def translate_personal_info(self, dataframe, extract, source_connect, target_connect, session,schema,logger,update_type, **kwargs):
        ''''
        独立函数,将wj_answer_master,wj_answer和map_table,wj_items等数据通过固定方式更新到personal_info,tj_record,tj_result中
        '''
        # 检查提交的任务中是否在同个源表中,存在wj_answer,map_table;
        # 检查提交的任务中的同个目标表中存在tj_record,tj_result;
        # 根据每一行的
        import re
        import pandas as pd
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        wj_answer_sql = "select * from {table} ".format(table="wj_answer")
        table2_sql1 = "select * from map_table where TARGET_TABLE = 'personal_info'"
        table2_sql2 = "select * from map_table where TARGET_TABLE = 'tj_result'"
        map_table_sql = "select * from map_table"
        wj_items_sql = "select * from wj_items"
        personal_finish_sql = "select * from personal_finish"
        empi_formula_sql = "select * from empi_formula"
        personal_info_sql = "select * from personal_info limit 1"
        tj_record_sql = "select * from tj_record limit 1"  # 只取字段名
        wj_answer_df = extract.read_mysql(wj_answer_sql, source_connect)
        tables2_df_1 = extract.read_mysql(table2_sql1, source_connect)
        tables2_df_2 = extract.read_mysql(table2_sql2, source_connect)
        map_table_df = extract.read_mysql(map_table_sql, source_connect)
        wj_items_df_3 = extract.read_mysql(wj_items_sql, source_connect)
        personal_finish_df = extract.read_mysql(personal_finish_sql, source_connect)
        empi_formula_df = extract.read_mysql(empi_formula_sql, source_connect)
        personal_info_df = extract.read_mysql(personal_info_sql, target_connect)
        tj_record_df = extract.read_mysql(tj_record_sql, target_connect)
        personal_info_columns = list(set([item for index, item in tables2_df_1["TARGET_FILED_ID"].iteritems()]))
        result_info_columns = list(set([item for index, item in tables2_df_2["TARGET_FILED_ID"].iteritems()]))
        record_df_columns = tj_record_df.columns.values.tolist()
        personal_finish_columns = personal_info_df.columns.values.tolist()
        personal_info_columns.append("OLD_ID")
        result_info_columns.append("DICT_CODE")  # 这个DICT_CODE目的是为了第二次同步时,作为查询是否重复的依据
        result_info_columns.append("WJ_ANSWER_MASTER_ID")
        personal_info_df = pd.DataFrame(columns=personal_info_columns)
        result_info_df = pd.DataFrame(columns=result_info_columns)
        tj_record_df = pd.DataFrame(columns=record_df_columns)

        engine = extract.create_mysql_engin__(source_connect)
        # new_wj_answer_master_df = dataframe
        # new_wj_answer_master_df = dataframe.query("ID >= 100337")
        # new_wj_answer_master_df = dataframe
        if update_type == 2:  # 是否增量更新
            now_date = datetime.datetime.now().strftime("%Y-%m-%d")
            now_date = datetime.datetime.strptime(now_date,"%Y-%m-%d")
            new_wj_answer_master_df = dataframe.query("CREATE_TIME >= %r" % now_date)  # 只更新当天
        else:
            new_wj_answer_master_df = dataframe
        count_skip = 0
        # print('new_wj_answer_master_df',new_wj_answer_master_df,sep='\r\n')
        for index, wj_answer_master_row in new_wj_answer_master_df.iterrows():  # 单个用户的问卷信息
            personal_info_tmp = {}
            tj_record_tmp = {}
            new_wj_answer_df = wj_answer_df.query('WJ_ANSWER_MASTER_ID == %s' % wj_answer_master_row["ID"])
            new_wj_answer_df = new_wj_answer_df.where(wj_answer_df.notnull(), None)
            # print("new_wj_answer_df1",new_wj_answer_df)
            '''
            由于数据源(wj_answer)在用户更新问卷答案后,问卷系统是通过删除原始数据,插入新的数据导致旧数据ID与新
            数据ID不一致,从而出现一个问卷对应重复答案.此处根据除WJ_ANSWER_MASTER_ID和ID外,将wj_answer中的相同
            项删除,在处理完毕后再讲相同的问卷问题(DICT_CODE)删除
            '''
            new_wj_answer_df.drop_duplicates(["WJ_LISTID","WJ_LIST_CONTENT_ID","ANSWER","QUESTION_TYPE",
                                              "QUESTION_CLASS"],keep='last',inplace=True)
            # print("new_wj_answer_df2",new_wj_answer_df)
            if len(new_wj_answer_df) == 0:
                count_skip += 1
            for table1_index, wj_answer_row in new_wj_answer_df.iterrows():  # 每一条问卷的信息
                # question_class = wj_answer_row.get("QUESTION_CLASS")
                tj_result_tmp = {}
                if int(wj_answer_row["QUESTION_CLASS"]) == 2:  # 如果问卷问题类型为2,则查询wj_items中是否存在对应的answer的值
                    answer = wj_answer_row["ANSWER"]
                    list_content_id = wj_answer_row["WJ_LIST_CONTENT_ID"]
                    try:
                        # result = str(answer).split(",")
                        # res = re.match(r"^(?P<num>[0-9]\d*)|(?P<list>\[.+?\])$", answer)  # 回答问题可能存在字符串或者列表两种情况
                        #res = re.match(r"^(?P<str>[0-9]\d*,[0-9]\d*)|(?P<num>[0-9]\d*)|(?P<list>\[.+?\])$", answer)  # 回答问题可能存在字符串或者列表两种情况
                        res = re.match(r"^(?P<num>[0-9]\d*$)|(?P<str>(\d+,)*\d+$)|(?P<list>\[.+?\]$)", answer)  # 回答问题可能存在字符串或者列表两种情况
                        if res:
                            if res.lastgroup == "num":
                                answer = int(res.group(0))
                            elif res.lastgroup == "list":
                                answer = eval(res.group(0))
                            elif res.lastgroup == "str":
                                ret = "[" + str(res.group(0)) + "]"
                                answer = eval(ret)
                            else:
                                answer = None
                        else:
                            print("answer:%s 无法被 re.match 匹配" % answer)
                            # continue
                    except Exception as s:
                        traceback.print_exc()
                        print("问卷表中出现不可转换的数据类型list_content_id:%s answer:%s ID:%s .该条问卷信息将被跳过" % (
                        list_content_id, answer, wj_answer_row["ID"]))
                        # continue  # 这条问卷信息跳出
                    all_res_df = []
                    type_check = ""
                    # print("answer", answer,"wj_answer_master_row['ID']",wj_answer_master_row["ID"],"list_content_id",list_content_id)
                    if type(answer) is int:
                        type_check = 'int'
                        res_df = wj_items_df_3.query('ID == %s' % answer).query(
                            'WJ_LIST_CONTENT_ID == %s' % list_content_id)
                        if len(res_df) > 0:
                            all_res_df.append(res_df)
                        else:
                            print("无法从tj_items表中获取 ID=%s , WJ_LIST_CONTENT_ID=%s 的数据" % (answer, list_content_id))
                    if type(answer) is list:
                        type_check = 'list'
                        for _answer in answer:
                            res_df = wj_items_df_3.query('ID == %s' % int(_answer)).query(
                                'WJ_LIST_CONTENT_ID == %s' % list_content_id)
                            if len(res_df) > 0:
                                all_res_df.append(res_df)
                            else:
                                print("无法从tj_items表中获取 ID=%s , WJ_LIST_CONTENT_ID=%s 的数据" % (
                                    answer, list_content_id))

                    for res_df in all_res_df:
                        for index, row in res_df.iterrows():  # 获取到wj_items中的真实回答问题
                            if row["WJ_LIST_CONTENT_ID"] and row["WJ_ITEMS_CONTENT_ID"]:
                                real_list_content_id = row["WJ_LIST_CONTENT_ID"]
                                real_answer = row["WJ_ITEMS_CONTENT_ID"]
                                # print("real_list_content_id",real_list_content_id,"real_answer",real_answer)
                                target_table2_df = map_table_df.query('TYPEID == 2').query(
                                    'SOURCE_FILED_ID == "%s"' % real_list_content_id).query(
                                    'SOURCE_FIELD_CODE == "%s"' % real_answer)
                                if len(target_table2_df) > 1:
                                    print("映射关系表中不能存在相同设置QUESTION_CLASS=2  real_list_content_id:%s real_answer:%s " % (
                                    real_list_content_id, real_answer))
                                    continue
                                if len(target_table2_df) < 0:
                                    print("无法获取映射表中的映射关系QUESTION_CLASS=2 real_list_content_id %s real_answer:%s " % (
                                    real_list_content_id, real_answer))
                                    continue
                                for index1, row1 in target_table2_df.iterrows():  # 在映射表中获取到映射关系
                                    if int(row1["CLASSID"]) == 1:
                                        TARGET_TABLE = row1.get("TARGET_TABLE")
                                        if TARGET_TABLE == "personal_info":  # 到个人信息库
                                            personal_info_tmp.setdefault(row1["TARGET_FILED_ID"],
                                                                         row1["TARGET_FILED_VALUE"])
                                            # tj_record_tmp.setdefault("DABH", DABH)
                                        if TARGET_TABLE == "tj_record":  # 到体检库
                                            # tj_record_tmp.setdefault("DABH", DABH)
                                            # tj_record_tmp.setdefault(row1["TARGET_FILED_ID"], answer)
                                            tj_record_tmp[row1["TARGET_FILED_ID"]] = answer
                                    elif int(row1["CLASSID"]) == 2:
                                        if type_check == 'int':
                                            tj_result_tmp.setdefault(row1["TARGET_FILED_ID"], row1["TARGET_FILED_VALUE"])
                                            # print("wj_answer_master_row: %s tj_result_tmp: %s " % (wj_answer_master_row["ID"],tj_result_tmp))
                                        elif type_check == 'list':
                                            print('tj_result_tmp',tj_result_tmp)
                                            tmp = tj_result_tmp.get(row1["TARGET_FILED_ID"])
                                            if tmp:
                                                tmp = tmp + "," + row1["TARGET_FILED_VALUE"]
                                                tj_result_tmp[row1["TARGET_FILED_ID"]] = tmp
                                            else:
                                                tj_result_tmp[row1["TARGET_FILED_ID"]] = row1["TARGET_FILED_VALUE"]
                                        if not tj_result_tmp.get("DICT_CODE"):
                                            tj_result_tmp["DICT_CODE"] = row1["TARGET_FILED_CODE"]
                                    break  # 只取第一条
                            # break  # 只取第一条
                if int(wj_answer_row["QUESTION_CLASS"]) == 1:  # 如果问卷问题类型为2,则查询wj_items中是否存在对应的answer的值
                    pd.set_option('display.max_rows', None)
                    answer = wj_answer_row["ANSWER"]
                    list_content_id = wj_answer_row["WJ_LIST_CONTENT_ID"]
                    target_map_table_df = map_table_df.query('TYPEID == "1"').query(
                        'SOURCE_FILED_ID == "%s"' % list_content_id)
                    if len(target_map_table_df) > 1:
                        print("映射关系表中不能存在相同设置QUESTION_CLASS=1 list_content_id %s answer:%s" % (list_content_id, answer))
                        # break
                    if len(target_map_table_df) <= 0:
                        print("无法获取映射表中的映射关系QUESTION_CLASS=1 list_content_id %s answer:%s" % (list_content_id, answer))
                        # break
                    for index1, row1 in target_map_table_df.iterrows():  # 在映射表中获取到映射关系
                        if int(row1["CLASSID"]) == 1:
                            TARGET_TABLE = row1.get("TARGET_TABLE")
                            if int(wj_answer_row["WJ_LIST_CONTENT_ID"]) == 9:  # 特殊处理:将年龄与数据创建日期转化为出生日期
                                if answer:
                                    birth_day = (datetime.datetime.strptime(datetime.datetime.strftime(
                                        wj_answer_master_row["CREATE_TIME"],"%Y-01-01"),"%Y-%m-%d") -
                                                 datetime.timedelta(days=int(answer) * 365)).strftime("%Y-01-01")
                                    personal_info_tmp.setdefault(row1["TARGET_FILED_ID"], birth_day)
                                else:
                                    personal_info_tmp.setdefault(row1["TARGET_FILED_ID"], None)
                                # tj_record_tmp.setdefault("DABH", DABH)
                            else:
                                if TARGET_TABLE == "personal_info":  # 到个人信息库
                                    personal_info_tmp.setdefault(row1["TARGET_FILED_ID"], answer)
                                    # tj_record_tmp.setdefault("DABH", DABH)
                                if TARGET_TABLE == "tj_record":  # 到体检库
                                    # tj_record_tmp.setdefault("DABH", DABH)
                                    # tj_record_tmp.setdefault(row1["TARGET_FILED_ID"], answer)
                                    tj_record_tmp[row1["TARGET_FILED_ID"]] = answer
                        elif int(row1["CLASSID"]) == 2:
                            tj_result_tmp.setdefault(row1["TARGET_FILED_ID"], wj_answer_row["ANSWER"])
                            if not tj_result_tmp.get("DICT_CODE"):
                                tj_result_tmp["DICT_CODE"] = row1["TARGET_FILED_CODE"]
                        break  # 只取第一条
                if tj_result_tmp:
                    id = wj_answer_master_row["ID"]
                    tj_result_tmp["WJ_ANSWER_MASTER_ID"] = id
                    new_result = pd.DataFrame(tj_result_tmp, index=[1])
                    if len(new_result) > 0:
                        result_info_df = result_info_df.append(new_result, ignore_index=True)
                if personal_info_tmp:
                    personal_info_tmp.setdefault("OLD_ID", wj_answer_master_row["ID"])
            if personal_info_tmp:
                # print("personal_info_tmp", personal_info_tmp)
                if not wj_answer_master_row.get("DABH"):
                    DABH = self.select_personal_finish(session, personal_info_tmp, empi_formula_df,
                                                       personal_finish_columns,personal_info_df,engine,schema,logger)
                    if not DABH:
                        DABH = self.create_DABH()
                else:
                    DABH = wj_answer_master_row.get("DABH")
                id = wj_answer_master_row["ID"]
                # print("已存在的DABH:", DABH)

                personal_info_tmp["DABH"] = DABH
                new = pd.DataFrame(personal_info_tmp, index=[1])
                personal_info_df = personal_info_df.append(new, ignore_index=True)

                # print("tj_record_tmp",tj_record_tmp)
                tj_record_tmp["DABH"] = DABH
                new_record = pd.DataFrame(tj_record_tmp, index=[1])
                tj_record_df = tj_record_df.append(new_record, ignore_index=True)

                if len(personal_info_df) > 0:
                    '''获取源数据中所有相同DABH的问卷条数'''
                    exist_personal_info_df = personal_info_df[personal_info_df.DABH == DABH]
                    '''找出历史有重复的问卷ID而且不是当前的问卷'''
                    for exist_index,exist_personal_row in exist_personal_info_df.iterrows():
                        exist_id = exist_personal_row.get("OLD_ID")
                        if exist_id != personal_info_tmp["OLD_ID"]:
                            '''将历史相同的问卷ID的对应的tj_result替换为当前的,目的是将同一个人的问卷信息合并到同一个tj_result档案信息中'''
                            result_info_df = result_info_df.replace({int(exist_id):int(id)})
        if len(result_info_df):
            result_info_df.drop_duplicates(["DICT_CODE","WJ_ANSWER_MASTER_ID"],keep='last',inplace=True)
            result_info_df = result_info_df.where(result_info_df.notnull(), None)
        # print("personal_info_df", personal_info_df, sep='\r\n')
        if len(personal_info_df):
            personal_info_df = personal_info_df.groupby(["DABH"]).apply(self.fileter_nan_to_ffill)  # 将nan替换为DABH相同的上一个数据
            personal_info_df.drop_duplicates(["DABH"], keep='last', inplace=True)
            personal_info_df = personal_info_df.where(personal_info_df.notnull(), None)
        if len(tj_record_df):
            tj_record_df = tj_record_df.groupby(["DABH"]).apply(self.fileter_nan_to_ffill)  # 将nan替换为DABH相同的上一个数据
            tj_record_df.drop_duplicates(["DABH"],keep='last',inplace=True)
            tj_record_df = tj_record_df.where(tj_record_df.notnull(),None)
        # print("personal_info_df", personal_info_df, sep='\r\n')
        # print("result_info_df", result_info_df, sep='\r\n')
        # print("tj_record_df", tj_record_df, sep='\r\n')
        # print("test_apply_detail_df", test_apply_detail_df, sep='\r\n')
        # print("tj_item_detail_df_1", tj_item_detail_df_1, sep='\r\n')
        # print("tj_items_df", tj_items_df, sep='\r\n')
        # print("personal_info_df", personal_info_df,sep='\r\n')
        # print("personal_info_df", personal_info_df[["CITIZEN_NAME","DABH","PHONE_NUMBER","GENDER","DATE_BIRTHDAY","NATIONALITY"]], sep='\r\n')
        # print("personal_info_df", personal_info_df[["OLD_ID","CITIZEN_NAME","DABH"]], sep='\r\n')
        # print("tj_record_df", tj_record_df[["ID_O","DABH","DIAGNOSE_CODE"]], sep='\r\n')
        # print("result_info_df", result_info_df[["WJ_ANSWER_MASTER_ID","CODE","CRESULT","DICT_CODE"]], sep='\r\n')
        # print("personal_info_df", personal_info_df[["CONTACT_PHONE_NUMBER"]], sep='\r\n')
        # return
        return personal_info_df, result_info_df, personal_info_columns, result_info_columns, \
               tj_record_df

    def fileter_nan_to_ffill(self,series):
        series1 = series.fillna(method="ffill")
        return series1

    def select_finish(self, session, data_dict,personal_info_df,engine,schema,logger,OLD_ID):
        '''
        数据的插入和更新前对数据的查询,由于本项目使用的是先将所有的数据处理好之后插入或更新到目标库中,
        所以如果只是查询目标库是否存在已有数据一种方式可能出现第一次插入时,本身目标库中没有重复,而源数据中
        有重复数据,但是当前数据还未插入到目标库中,所以需要两步查询,避免由于数据源重复的情况产生两条数据.

        出现问题1:数据源存在重复情况,此处如果返回DABH后会被传递到下一个步骤loader(下一个步骤需要更新DABH到数据源
        的DABH)到目标库中,但是,此时更新数据源的DABH没有问题,如果插入或更新目标库出现两个相同的DABH便会出现双主键情况.
        解决方案1:再判断数据源出现重复情况后,先更新数据源的DABH,然后传递到下一步骤的df不新增该条数据,下一步骤也不需要
        更新数据源的DABH,只负责查询目标库是否存在DABH,根据结果插入或更新

        出现问题2:更新或者插入到目标库的数据中,personal_info的数据的CITIZEN_NAME来自wj_answer,而wj_answer_master的ANSWER_NAME
        与该条数据的CITIZEN_NAME不一致

        出现问题3:当所有的问卷信息在打分分组中没有匹配到对应的组别,此时会生成一个新的DABH,此时数据源的数据也更新到当前的DABH,但是
        如果出现一个与该问卷信息一样的,而且也是没有组别匹配,会生成一个新的DABH,将其默认为新的数据

        出现问题4:当出现当前数据与源数据重复时(通过select_finish函数判断完成),需要将其问卷的answer更新
        '''
        sql = 'select * from %s where ' % "personal_info" + " and ".join(['%s=%r' %(key,value) for key,value in data_dict.items()])
        reult = self.select_mysql_data(session,sql)
        if len(reult) > 0:  # 在目标库中已存在
            print("source_personal_info_data_dict1",data_dict)
            old_id = OLD_ID
            info_DABH = reult[0][0]
            where = ("ID", old_id)  # 再transform时将OLD_ID添加到personal_info中,更新完wj_answer_master将其删除
            self.update_mysql_data(engine, schema, "wj_answer_master", {"DABH": info_DABH}, where, logger)
            return reult[0][0]
        else:
            print("target_personal_info_data_dict",data_dict)
            conditions = []
            for k, v in data_dict.items():
                conditions.append("(personal_info_df." + k + " == " + "%r" % v + ")")
            condition = " & ".join(conditions)
            print("condition",condition)
            old_personal_info = personal_info_df[eval(condition)]
            # print('old_personal_info',old_personal_info)
            if len(old_personal_info) > 0:  # 再中间库中已存在
                # print("data_dict", data_dict)
                info_DABH = None
                # print('old_personal_info', old_personal_info)
                for index, row in old_personal_info.iterrows():
                    # 更新wj_answer_master表的DABH
                    print("更新ID:%s 数据,名称:%s ,DABH:%s"% (row.get("OLD_ID"),row.get("CITIZEN_NAME"),row.get("DABH")))
                    old_id = row.get("OLD_ID")
                    info_DABH = row.get("DABH")
                    where = ("ID", old_id)  # 再transform时将OLD_ID添加到personal_info中,更新完wj_answer_master将其删除
                    self.update_mysql_data(engine, schema, "wj_answer_master", {"DABH": info_DABH}, where, logger)
                # return is_update
                return info_DABH
            else:
                return None

    def get_grade(self,group_id,empi_formula_group_df):
        try:
            grade = 0
            for _group_id,group in empi_formula_group_df:
                if _group_id == group_id:
                    for index,row in group.iterrows():
                        grade += row.get("SCORE")
            return grade
        except Exception as e:
            traceback.print_exc()

    def select_personal_finish(self, session, field_dict, empi_formula_df, personal_finish_columns,personal_info_df
                               ,engine,schema,logger):
        '''
        档案数据库的去重策略
        1,判断身份证号
        2,判断手机号+姓名
        3,判断
        '''

        # 身份证号:ID_CARD_NO
        # field_dict = {'CITIZEN_NAME': '陈小雪', 'OLD_ID': 100137, 'DATE_BIRTHDAY': '1978-01-02', 'CONTACT_PHONE_NUMBER': '14559356781'}
        # field_dict = {'CITIZEN_NAME': '徐建国', 'OLD_ID': 100119, 'GENDER': '1', 'NATIONALITY': '汉族', 'DATE_BIRTHDAY': '1990-12-27', 'REG_ADDRESS_CODE': '["安徽省","六安市","金寨县"]', 'RES_ADDRESS_CODE': '["安徽省","合肥市","包河区"]'}
        # field_dict = {'RES_ADDRESS_CODE': None, 'NATIONALITY': None, 'CITIZEN_NAME': '姓名vv', 'PHONE_NUMBER': None, 'EMAIL': None, 'DATE_BIRTHDAY': '1987-03-01', 'REG_ADDRESS_CODE': None, 'GENDER': '1', 'OLD_ID': 100243, 'DABH': '20200701183548501772'}
        print("field_dict",field_dict)
        THROUGHT_RATE = 80  # 判断各个允许字段的总通过率是否到达该标准
        empi_formula_lines = empi_formula_df.groupby("GROUP_ID")
        allow_filed = list(key for key, value in field_dict.items() if value and key and key in personal_finish_columns) # 这条数据被允许插入或更新的字段
        group_field_list = [{group_id: [row.get("FIELD_CODE") for index, row in group.iterrows()]} for group_id, group in empi_formula_lines]  # 评分标准的组对应的所有字段
        # print("allow_filed", allow_filed)
        # print("group_field_list", group_field_list)
        for item in group_field_list:  # [{0: ['ID_CARD_NO']}, {1: ['PHONE_NUMBER', 'CITIZEN_NAME']}, {2: ['CITIZEN_NAME', 'GENDER', 'DATE_BIRTHDAY']}]
            for group_id,value in item.items():
                # print("当前组别:",group_id)
                check_list = []
                new_dict = {}
                for filed in value:
                    if filed in allow_filed:
                        check_list.append(filed)
                        new_dict[filed] = field_dict.get(filed)
                if len(check_list) == len(value):  # 该条数据在打分标准字段中的所有字段都出现
                    # print("item.values()",value)
                    # print("check_new_dict",new_dict)
                    OLD_ID = field_dict.get("OLD_ID")
                    exist_DABH = self.select_finish(session,new_dict,personal_info_df,engine,schema,logger,OLD_ID)
                    # print("exist_DABH",exist_DABH)
                    # if exist_DABH != "update":
                    if exist_DABH:
                        # if exist_DABH:
                            # print("exist_DABH",exist_DABH)
                        grade = self.get_grade(group_id,empi_formula_lines)
                        if grade >= THROUGHT_RATE:
                            return exist_DABH
        else:
            print("所有组别都无法匹配到该条信息:%s"% field_dict)
                # else:

                    # return None
        return None

    def create_DABH(self):
        '''生成档案编号'''
        now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        DABH = now + "%000000s" % random.randint(111111, 999999)
        return DABH

    def translate_test(self,dataframe,extract, source_connect,target_connect, session,schema,logger,update_type):
        '''
        独立的函数,通过中间库主表personal_acceptinfo,次表sample_info(主表和次表均从视图更新,不存在外键关联)将数据更新到正式库的test_info,test_sample_info中,
        其中:正式库中的test_info表的ID_O获取方式：personal_acceptinfo表的WJID从wj_answer_master表找到DABH，
            通过DABH从正式库的tj_record拿到ID_O

        步骤:
            1.创建空的test_info_df,test_sample_info_df和已存在的wj_answer_master_df,wj_record_df,sample_info_df;
            2.以df方式循环主表personal_acceptinfo,获取每一条检测信息;
                a.通过WJID在中间库wj_answer_master_df中获取对应的DABH,然后通过DABH在wj_record_df中获取对应的ID_O;
                b.将ID_O和personal_acceptiinfo_df存在相同字段的数据添加到空的test_info_df,
                    并将sample_info_df更新到空的test_sample_info_df(通过sample_info_df中与personal_acceptinfo_df中相同的UID);
            3.将新的test_info_df,test_sample_info_df插入或更新到目标库中

        说明:
            dataframe是主表personal_acceptinfo的df
        '''
        # 步骤1:
        import pandas as pd
        pd.set_option('display.max_rows', None)
        test_info_sql = "select * from test_info"
        test_sample_sql = "select * from test_sample_info"
        wj_answer_master_sql = "select * from wj_answer_master"
        wj_record_sql = "select * from tj_record"
        sample_info_sql = "select * from sample_info"
        test_info_df = extract.read_mysql(test_info_sql, target_connect)   # 目标库
        test_sample_info_df = extract.read_mysql(test_sample_sql, target_connect)  # 目标库
        wj_answer_master_df = extract.read_mysql(wj_answer_master_sql, source_connect)  # 中间库
        wj_record_df = extract.read_mysql(wj_record_sql, target_connect)  # 目标库
        sample_info_df = extract.read_mysql(sample_info_sql, source_connect)  # 中间库
        test_info_columns = test_info_df.columns.values.tolist()  # 通过已有的表获取表字段
        test_sample_info_columns = test_sample_info_df.columns.values.tolist()  # 通过已有的表获取表字段

        test_info_df = pd.DataFrame(columns=test_info_columns)  # 通过字段创建空的df
        test_sample_info_df = pd.DataFrame(columns=test_sample_info_columns)  # 通过字段创建空的df

        # 步骤2:
        if update_type == 2:  # 是否增量更新
            now_date = datetime.datetime.now().strftime("%Y-%m-%d")
            now_date = datetime.datetime.strptime(now_date, "%Y-%m-%d")
            new_dataframe = dataframe.query("CREATE_TIME >= %r" % now_date)  # 只更新当天
        else:
            new_dataframe = dataframe
        for personal_index,personal_accept_info_row in new_dataframe.iterrows():  # personal_acceptinfo主表
            wj_id = personal_accept_info_row.get("WJID")
            uid = personal_accept_info_row.get("UID")
            answer_master_df = wj_answer_master_df.query("ID == %s" % wj_id)
            for answer_index,answer_master_df_row in answer_master_df.iterrows():
                dabh = answer_master_df_row.get("DABH")
                print('dabh',dabh)
                if not dabh:
                    continue
                record_df = wj_record_df.query("DABH == '%s'" % dabh)
                # print('record_df',record_df,sep='\r\n')
                for record_index,record_df_row in record_df.iterrows():
                    id_o = record_df_row.get("ID_O")
                    personal_accept_info_dict = dict(personal_accept_info_row)
                    personal_accept_info_dict["ID_O"] = id_o
                    # print('id_o',id_o,"personal_accept_info_dict",personal_accept_info_dict)
                    if personal_accept_info_dict:
                        test_info_new = pd.DataFrame(personal_accept_info_dict, index=[1])
                        test_info_df = test_info_df.append(test_info_new, ignore_index=True)

                        # 通过uid获取到对应的sample_info
                        sample_info = sample_info_df.query("UID == '%s'" % uid)
                        for sample_index,sample_info_row in sample_info.iterrows():
                            sample_info_dict = dict(sample_info_row)
                            sample_info_new = pd.DataFrame(sample_info_dict, index=[1])
                            test_sample_info_df = test_sample_info_df.append(sample_info_new, ignore_index=True)
        # print("test_info_df",test_info_df,sep='\r\n')
        # print("test_sample_df",test_sample_info_df,sep='\r\n')
        return test_info_df,test_sample_info_df

    def select_mysql_data(self, session, sql_comment):
        cursor = session.execute(sql_comment)
        result = cursor.fetchall()
        return result

    def update_mysql_data(self, engine, schema, table_name, data_dict, where, logger):
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
            file_path = os.path.join(sys.path[0], "logs",
                                     "%s.log" % datetime.datetime.strftime(datetime.datetime.now(), "%Y_%m_%d"))
            log = logger(filename=file_path)
            log.removeHandler(log.handlers)
            log.info(e.__str__())
            traceback.print_exc()
            return None

    def methods__(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self
