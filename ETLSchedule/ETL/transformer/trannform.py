#
import pandas as pd
import datetime,random,traceback
import numpy as np


class BaseTransForm(object):

    def __init__(self):
        self.mapping_dict = {"module_id":1,"primary_key":False}
        self.split_data_dict = {"module_id":2,"primary_key":False}
        self.select_primary_key_dict = {"module_id":3,"primary_key":True}
        self.transform_type_dict = {"module_id":4,"primary_key":False}
        self.virtual_field_dict = {"module_id":5,"primary_key":False}
        self.none_transfrom_value_dict = {"module_id":4,"primary_key":False}
        self.trans_position_dict = {"module_id":4,"primary_key":False}

    def mapping(self,dataframe,column,from_data,to_data,to_column):
        '''将某一列的值进行映射,column:列名,from_data:源数据,to_data:目标数据,to_column:目标列,如果from_data或者to_data为空则不做任何处理'''

        if from_data is None or to_data is None:
            return dataframe
        dataframe[column] = dataframe[column].map(lambda x: to_data if x == from_data else x)
        return dataframe

    def split_data(self,dataframe,column,sep,to_column):
        '''切分数据 column:字段名,sep:分割符,to_column:目标列'''
        dataframe[column] = dataframe[column].map(lambda x:str(x).split(sep)[0] if x is not None else x)
        return dataframe

    def select_primary_key(self,dataframe,column,to_column):
        '''选择字段作为装载数据时的判断是否重复的依据(每个任务必填),
        此处仅提供可视化选择对应的参数列表,实际处理在loader模块,column:源列名,to_column:目标列'''
        return dataframe

    def transform_type(self,dataframe,column,target_type):
        '''类型转换函数,column:要进行转换的列,target_type:要转换成的类型,如果类型转换到目标库时失败任务将停止'''
        dataframe[column] = dataframe[column].map(lambda x:eval(target_type)(x))
        return dataframe

    def virtual_field(self,dataframe,virtual_column,to_column):
        '''虚拟字段,虚拟一个源字段并将该字段的数据写入到指定的目标列中,virtual_column:虚拟字段值,to_column:目标列'''
        dataframe["virtual_column"] = ''
        dataframe["virtual_column"] = dataframe["virtual_column"].map(lambda x: virtual_column)
        return dataframe

    def none_transfrom_value(self,dataframe,column,value):
        '''空值处理,column:要进行转换的列,value:将空值替换成的数值'''
        dataframe[column] = dataframe[column].map(lambda x: value
                        if type(x) is pd._libs.tslibs.nattype.NaTType or x is None else x)
        return dataframe

    def judge_row_similarity(self,dataframe,column_list):
        '''处理两行数据之间是否存在相似关系 column_list:要进行比较的列'''
        # 比较要写入到目标库中的行数据和已存在目标库中的数据是否已存在
        pass

    def trans_position(self,dataframe):
        '''将数据进行转置,将行数据转变成列数据,将列名转置为行数据'''
        dataframe = dataframe.stack()
        return dataframe

    def build_record_numb(self):
        '''生成档案编号,'''

        # 一,判断该行数据的是否存在档案编号列,(档案编号列名)
        #   存在:直接将数据更新到目标库(根据档案编号)
        # 二,不存在就将根据规则生成档案号(源列名,生成规则)

    def translate(self,dataframe,extract,source_connect,**kwargs):
        ''''
        source_table1':'wj_answer','source_table2':'wj_items','source_table3':'wj_items_content',
        'target_table1':'dict_code','target_table2':'dict_detail','source_table1_column1': 'WJ_LIST_CONTENT_ID',
        'source_column':'ID','table1_condition_id':'ID','source_table1_column2':'WJ_LISTID', 'to_column': 'ANSWER'
        '''
        # 检查提交的任务中是否在同个源表中,存在wj_answer,map_table;
        # 检查提交的任务中的同个目标表中存在tj_record,tj_result;
        # 根据每一行的
        import re
        import pandas as pd
        pd.set_option('display.max_rows', None)
        wj_answer_sql = "select * from {table} ".format(table="wj_answer")
        table2_sql1 = "select * from map_table where TARGET_TABLE = 'personal_info'"
        table2_sql2 = "select * from map_table where TARGET_TABLE = 'tj_result'"
        map_table_sql = "select * from map_table"
        wj_items_sql = "select * from wj_items"
        # test_apply_sql = 'select * from test_apply where WJID != "NULL";'
        tj_item_detail_sql = "select * from tj_item_detail"
        wj_answer_master_sql = "select * from wj_answer_master;"
        test_apply_detail_sql = "select * from test_apply_detail"
        tj_record_sql = "select * from tj_record limit 1"  # 只取字段名
        wj_answer_df = extract.read_mysql(wj_answer_sql, source_connect)
        tables2_df_1 = extract.read_mysql(table2_sql1, source_connect)
        tables2_df_2 = extract.read_mysql(table2_sql2, source_connect)
        map_table_df = extract.read_mysql(map_table_sql, source_connect)
        wj_items_df_3 = extract.read_mysql(wj_items_sql, source_connect)
        wj_answer_master_df = extract.read_mysql(wj_answer_master_sql,source_connect)
        tj_item_detail_df = extract.read_mysql(tj_item_detail_sql,source_connect)
        test_apply_detail_df = extract.read_mysql(test_apply_detail_sql,source_connect)
        tj_record_df = extract.read_mysql(tj_record_sql,source_connect)
        personal_info_columns = list(set([item for index,item in tables2_df_1["TARGET_FILED_ID"].iteritems()]))
        result_info_columns = list(set([item for index,item in tables2_df_2["TARGET_FILED_ID"].iteritems()]))
        record_df_columns = tj_record_df.columns.values.tolist()
        tj_items_columns = list(set(dataframe.columns.values.tolist()))
        tj_item_detail_columns = list(set(tj_item_detail_df.columns.values.tolist()))
        personal_info_columns.append("OLD_ID")
        tj_items_columns.append("OLD_ID")
        result_info_columns.append("DICT_CODE")  # 这个DICT_CODE目的是为了第二次同步时,作为查询是否重复的依据
        personal_info_df = pd.DataFrame(columns=personal_info_columns)
        result_info_df = pd.DataFrame(columns=result_info_columns)
        tj_record_df = pd.DataFrame(columns=record_df_columns)
        tj_items_df = pd.DataFrame(columns=tj_items_columns)
        tj_item_detail_df_1 = pd.DataFrame(columns=tj_item_detail_columns)
        dataframe.drop_duplicates("NAME",inplace=True,keep='last')
        for index,apply_row in dataframe.iterrows():
            # print(apply_row,"===",sep="\r\n")
            wj_id = apply_row["WJID"]
            new_wj_answer_master_df = wj_answer_master_df.query('ID == %s' % wj_id)
            if not apply_row["DABH"]:  # 以test_apply表为主,根据DABH中的字段是否存在值,存在则不改变原档案编号,不存则创建新的档案编号
                DABH = self.create_DABH()
            else:
                DABH = apply_row["DABH"]
            if len(new_wj_answer_master_df) > 0:
                personal_info_tmp = {}
                tj_record_tmp = {}
                for index,source_column_1 in new_wj_answer_master_df.iterrows():  # 单个用户的问卷信息
                    new_wj_answer_df = wj_answer_df.query('WJ_ANSWER_MASTER_ID == %s' % source_column_1["ID"])
                    new_wj_answer_df = new_wj_answer_df.where(wj_answer_df.notnull(), None)
                    for table1_index, wj_answer_row in new_wj_answer_df.iterrows():  # 每一条问卷的信息
                        tj_result_tmp = {}
                        if wj_answer_row["QUESTION_CLASS"] == 2:  # 如果问卷问题类型为2,则查询wj_items中是否存在对应的answer的值
                            answer = wj_answer_row["ANSWER"]
                            list_content_id = wj_answer_row["WJ_LIST_CONTENT_ID"]
                            # print('answer:',answer,'type:',type(answer))
                            try:
                                res = re.match(r"^(?P<num>[0-9]\d+)|(?P<list>\[.+?\])$", answer) # 回答问题可能存在字符串或者列表两种情况
                                if res:
                                    if res.lastgroup == "num":
                                        answer = int(res.group(0))
                                    if res.lastgroup == "list":
                                        answer = eval(res.group(0))
                            except Exception as s:
                                traceback.print_exc()
                                print("问卷表中出现不可转换的数据类型list_content_id:%s answer:%s ID:%s .该条问卷信息将被跳过" % (list_content_id,answer,wj_answer_row["ID"]))
                                continue  # 这条问卷信息跳出
                            all_res_df = []
                            if answer and type(answer) is int:
                                res_df = wj_items_df_3.query('ID == "%s"' % answer).query('WJ_LIST_CONTENT_ID == "%s"' % list_content_id)
                                if len(res_df) > 0:
                                    all_res_df.append(res_df)
                            if answer and type(answer) is list:
                                for _answer in answer:
                                    res_df = wj_items_df_3.query('ID == "%s"' % _answer).query(
                                        'WJ_LIST_CONTENT_ID == "%s"' % list_content_id)
                                    if len(res_df) > 0:
                                        all_res_df.append(res_df)
                            for res_df in all_res_df:
                                for index,row in res_df.iterrows():  # 获取到wj_items中的真实回答问题
                                    if row["WJ_LIST_CONTENT_ID"] and row["WJ_ITEMS_CONTENT_ID"]:
                                        real_list_content_id = row["WJ_LIST_CONTENT_ID"]
                                        real_answer = row["WJ_ITEMS_CONTENT_ID"]
                                        target_table2_df = map_table_df.query('TYPEID == 2').query('SOURCE_FILED_ID == "%s"' % real_list_content_id).query('SOURCE_FIELD_CODE == "%s"' % real_answer)
                                        if len(target_table2_df) > 1:
                                            print("映射关系表中不能存在相同设置QUESTION_CLASS=2  real_list_content_id:%s real_answer:%s " % (real_list_content_id,real_answer))
                                            # break
                                        if len(target_table2_df) < 0:
                                            print("无法获取映射表中的映射关系QUESTION_CLASS=2 real_list_content_id %s real_answer:%s " % (real_list_content_id,real_answer))
                                            # break
                                        for index1,row1 in target_table2_df.iterrows():  # 在映射表中获取到映射关系
                                            if row1["CLASSID"] == 1:
                                                TARGET_TABLE = row1.get("TARGET_TABLE")
                                                if TARGET_TABLE == "personal_info":  # 到个人信息库
                                                    personal_info_tmp.setdefault(row1["TARGET_FILED_ID"], row1["TARGET_FILED_VALUE"])
                                                    tj_record_tmp.setdefault("DABH", DABH)
                                                if TARGET_TABLE == "tj_record":  # 到体检库
                                                    tj_record_tmp.setdefault("DABH", DABH)
                                                    tj_record_tmp.setdefault(row1["TARGET_FILED_ID"], answer)
                                            elif row1["CLASSID"] == 2:
                                                tj_result_tmp.setdefault(row1["TARGET_FILED_ID"], row1["TARGET_FILED_VALUE"])
                                                if not tj_result_tmp.get("DICT_CODE"):
                                                    tj_result_tmp["DICT_CODE"] = row1["TARGET_FILED_CODE"]
                                            break  # 只取第一条
                                    break  # 只取第一条

                        if wj_answer_row["QUESTION_CLASS"] == 1:  # 如果问卷问题类型为2,则查询wj_items中是否存在对应的answer的值
                            pd.set_option('display.max_rows', None)
                            answer = wj_answer_row["ANSWER"]
                            list_content_id = wj_answer_row["WJ_LIST_CONTENT_ID"]
                            target_table2_df = map_table_df.query('TYPEID == "1"').query('SOURCE_FILED_ID == "%s"' % list_content_id)
                            if len(target_table2_df) > 1:
                                print("映射关系表中不能存在相同设置QUESTION_CLASS=1 list_content_id %s answer:%s" % (list_content_id,answer))
                                # break
                            if len(target_table2_df) <= 0:
                                print("无法获取映射表中的映射关系QUESTION_CLASS=1 list_content_id %s answer:%s" % (list_content_id,answer))
                                # break
                            for index1, row1 in target_table2_df.iterrows():  # 在映射表中获取到映射关系
                                if row1["CLASSID"] == 1:
                                    TARGET_TABLE = row1.get("TARGET_TABLE")
                                    if int(wj_answer_row["WJ_LIST_CONTENT_ID"]) == 9:  # 特殊处理:将年龄与数据创建日期转化为出生日期
                                        birth_day = (source_column_1["CREATE_TIME"] - datetime.timedelta(days=int(answer) * 365)).strftime("%Y-%m-%d %H:%M:%S")
                                        personal_info_tmp.setdefault(row1["TARGET_FILED_ID"],birth_day)
                                        tj_record_tmp.setdefault("DABH", DABH)
                                    else:
                                        if TARGET_TABLE == "personal_info":  # 到个人信息库
                                            personal_info_tmp.setdefault(row1["TARGET_FILED_ID"], answer)
                                            tj_record_tmp.setdefault("DABH", DABH)
                                        if TARGET_TABLE == "tj_record":  # 到体检库
                                            tj_record_tmp.setdefault("DABH", DABH)
                                            tj_record_tmp.setdefault(row1["TARGET_FILED_ID"], answer)
                                elif row1["CLASSID"] == 2:
                                    tj_result_tmp.setdefault(row1["TARGET_FILED_ID"], wj_answer_row["ANSWER"])
                                    if not tj_result_tmp.get("DICT_CODE"):
                                        tj_result_tmp["DICT_CODE"] = row1["TARGET_FILED_CODE"]
                                break  # 只取第一条
                        if tj_result_tmp:
                            tj_result_tmp["DABH"] = DABH
                            new2 = pd.DataFrame(tj_result_tmp, index=[1])
                            result_info_df = result_info_df.append(new2, ignore_index=True)
                        if personal_info_tmp:
                            personal_info_tmp.setdefault("OLD_ID", source_column_1["ID"])
                if personal_info_tmp:
                    personal_info_tmp["DABH"] = DABH
                    new = pd.DataFrame(personal_info_tmp, index=[1])
                    personal_info_df = personal_info_df.append(new,ignore_index=True)
                if tj_record_tmp:
                    # tj_record_tmp["DABH"] = DABH
                    new_record = pd.DataFrame(tj_record_tmp, index=[1])
                    tj_record_df = tj_record_df.append(new_record, ignore_index=True)
            tj_tiems_dict = dict(apply_row)
            tj_tiems_dict["DABH"] = DABH
            tj_tiems_dict.setdefault("OLD_ID",apply_row.get("ID"))
            new_tj_tiems = pd.DataFrame(tj_tiems_dict, index=[1])
            tj_items_df = tj_items_df.append(new_tj_tiems,ignore_index=True)
            test_apply_onece = test_apply_detail_df.query('APPLYID == %s' % tj_tiems_dict.get('ID'))
            if len(test_apply_onece) > 0:
                # print('len(test_apply_onece)',len(test_apply_onece))
                for index,test_apply in test_apply_onece.iterrows():
                    APPLYID = test_apply.APPLYID
                    SAMPLE_NO = test_apply.SAMPLE_NO
                    SAMPLE_TYPE = test_apply.SAMPLE_TYPE
                    REMARK = test_apply.REMARK
                    test_apply_onece_dict = {"TJ_ITEMID":APPLYID,"SAMPLE_NO":SAMPLE_NO,"SAMPLE_TYPE":SAMPLE_TYPE,"REMARK":REMARK}
                    # print("test_apply_onece_dict",test_apply_onece_dict)
                    new_test_apply_onece = pd.DataFrame(test_apply_onece_dict,index=[1])
                    # print("new_test_apply_onece",new_test_apply_onece)
                    if len(new_test_apply_onece):
                        tj_item_detail_df_1 = tj_item_detail_df_1.append(new_test_apply_onece,ignore_index=True)
        # print("personal_info_df",personal_info_df,sep='\r\n')
        # print("result_info_df",result_info_df,sep='\r\n')
        # print("record_df",record_df,sep='\r\n')

        # values = {"NATIONALITY": "", "GENDER": 0, "CITIZEN_NAME": ""}
        # personal_info_df = personal_info_df.fillna(value=values)
        # values = {"CRESULT": "", "CODE": "", "DICT_CODE": ""}
        # result_info_df = result_info_df.fillna(value=values)
        test_apply_detail_df = test_apply_detail_df.where(test_apply_detail_df.notnull(),None)
        tj_item_detail_df_1 = tj_item_detail_df_1.where(tj_item_detail_df_1.notnull(),None)
        tj_items_df = tj_items_df.where(tj_items_df.notnull(),None)
        personal_info_df = personal_info_df.where(personal_info_df.notnull(),None)
        result_info_df = result_info_df.where(result_info_df.notnull(),None)
        tj_record_df = tj_record_df.where(tj_record_df.notnull(),None)
        # print("result_info_df", result_info_df, sep='\r\n')
        # print("test_apply_detail_df", test_apply_detail_df, sep='\r\n')
        # print("tj_item_detail_df_1", tj_item_detail_df_1, sep='\r\n')
        # print("tj_items_df", tj_items_df, sep='\r\n')
        # print("tj_record_df", tj_record_df, sep='\r\n')
        # print("personal_info_df", personal_info_df[["DATE_BIRTHDAY"]], sep='\r\n')
        return personal_info_df,result_info_df,personal_info_columns,result_info_columns,\
               tj_item_detail_df_1,tj_items_df,tj_record_df

    def create_DABH(self):
        '''生成档案编号'''
        now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        DABH = now + "%000000s"%random.randint(111111,999999)
        return DABH

    def methods__(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))


class AttrDict(dict):
    def __init__(self,*args,**kwargs):
        super(AttrDict,self).__init__(*args,**kwargs)
        self.__dict__ = self