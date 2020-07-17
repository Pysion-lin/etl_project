import traceback



def translate_personal_info(dataframe, extract, source_connect, target_connect, session, schema, logger):
    ''''
    独立函数,将wj_answer_master,wj_answer和map_table,wj_items等数据通过固定方式更新到personal_info,tj_record,tj_result中
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
    personal_finish_sql = "select * from personal_finish"
    empi_formula_sql = "select * from empi_formula"
    personal_info_sql = "select * from personal_info limit 1"
    # test_apply_sql = 'select * from test_apply where WJID != "NULL";'
    # tj_item_detail_sql = "select * from tj_item_detail"
    # wj_answer_master_sql = "select * from wj_answer_master;"
    # test_apply_detail_sql = "select * from test_apply_detail"
    tj_record_sql = "select * from tj_record limit 1"  # 只取字段名
    wj_answer_df = extract.read_mysql(wj_answer_sql, source_connect)
    tables2_df_1 = extract.read_mysql(table2_sql1, source_connect)
    tables2_df_2 = extract.read_mysql(table2_sql2, source_connect)
    map_table_df = extract.read_mysql(map_table_sql, source_connect)
    wj_items_df_3 = extract.read_mysql(wj_items_sql, source_connect)
    personal_finish_df = extract.read_mysql(personal_finish_sql, source_connect)
    empi_formula_df = extract.read_mysql(empi_formula_sql, source_connect)
    personal_info_df = extract.read_mysql(personal_info_sql, target_connect)
    # wj_answer_master_df = extract.read_mysql(wj_answer_master_sql,source_connect)
    # tj_item_detail_df = extract.read_mysql(tj_item_detail_sql,source_connect)
    # test_apply_detail_df = extract.read_mysql(test_apply_detail_sql,source_connect)
    tj_record_df = extract.read_mysql(tj_record_sql, target_connect)
    personal_info_columns = list(set([item for index, item in tables2_df_1["TARGET_FILED_ID"].iteritems()]))
    result_info_columns = list(set([item for index, item in tables2_df_2["TARGET_FILED_ID"].iteritems()]))
    record_df_columns = tj_record_df.columns.values.tolist()
    # personal_finish_columns = personal_finish_df.columns.values.tolist()
    # tj_items_columns = list(set(dataframe.columns.values.tolist()))
    # tj_item_detail_columns = list(set(tj_item_detail_df.columns.values.tolist()))
    personal_finish_columns = personal_info_df.columns.values.tolist()
    personal_info_columns.append("OLD_ID")
    # tj_items_columns.append("OLD_ID")
    result_info_columns.append("DICT_CODE")  # 这个DICT_CODE目的是为了第二次同步时,作为查询是否重复的依据
    result_info_columns.append("WJ_ANSWER_MASTER_ID")
    personal_info_df = pd.DataFrame(columns=personal_info_columns)
    result_info_df = pd.DataFrame(columns=result_info_columns)
    tj_record_df = pd.DataFrame(columns=record_df_columns)
    tj_record_df.to_sql()
    tj_record_df.merge()
    engine = extract.create_mysql_engin__(source_connect)
    new_wj_answer_master_df = dataframe
    count_skip = 0
    for index, wj_answer_master_row in new_wj_answer_master_df.iterrows():  # 单个用户的问卷信息

        personal_info_tmp = {}
        tj_record_tmp = {}
        new_wj_answer_df = wj_answer_df.query('WJ_ANSWER_MASTER_ID == %s' % wj_answer_master_row["ID"])
        new_wj_answer_df = new_wj_answer_df.where(wj_answer_df.notnull(), None)
        if len(new_wj_answer_df) == 0:
            count_skip += 1
        for table1_index, wj_answer_row in new_wj_answer_df.iterrows():  # 每一条问卷的信息
            tj_result_tmp = {}
            if int(wj_answer_row["QUESTION_CLASS"]) == 2:  # 如果问卷问题类型为2,则查询wj_items中是否存在对应的answer的值
                answer = wj_answer_row["ANSWER"]
                list_content_id = wj_answer_row["WJ_LIST_CONTENT_ID"]
                try:
                    # result = str(answer).split(",")
                    res = re.match(r"^(?P<num>[0-9]\d*)|(?P<list>\[.+?\])$", answer)  # 回答问题可能存在字符串或者列表两种情况
                    if res:
                        if res.lastgroup == "num":
                            answer = int(res.group(0))
                        if res.lastgroup == "list":
                            answer = eval(res.group(0))
                    else:
                        print("answer:%s 无法被 re.match 匹配" % answer)
                        # continue
                except Exception as s:
                    traceback.print_exc()
                    print("问卷表中出现不可转换的数据类型list_content_id:%s answer:%s ID:%s .该条问卷信息将被跳过" % (
                        list_content_id, answer, wj_answer_row["ID"]))
                    # continue  # 这条问卷信息跳出

                '''
                1.循环wj_answer,获取每一条wj_answer信息
                2.获取answer数据使用re匹配,得到三种类型.
                    tj_result_dict = {}
                    a.int类型
                        tj_result_dict{'CRESULT':转换后的数据,'CODE':转换后的code,'DICT_CODE':从map_table中获取的DICT_CODE}
                    b.list类型
                        tj_result_dict{'CRESULT':None,'CODE':'转换后的code1,转换后的code2,...','DICT_CODE':从map_table中获取的DICT_CODE}
                    c.str类型
                        tj_result_dict{'CRESULT':None,'CODE':'转换后的code1,转换后的code2,...','DICT_CODE':从map_table中获取的DICT_CODE}
                    注:定义一个方法,input:answer,output:CRESULT,CODE,DICT_CODE
                '''
                question_class = wj_answer_row.get("QUESTION_CLASS")
                all_res_df = []
                if type(answer) is int:
                    dict = get_answer_map(wj_items_df_3, map_table_df, answer, list_content_id, question_class)
                    if dict:
                        print(dict)
                    else:
                        print("无法从tj_items表中获取 ID=%s , WJ_LIST_CONTENT_ID=%s 的数据" % (answer, list_content_id))
                if type(answer) is list:
                    for _answer in answer:
                        print("_answer", _answer, type(_answer))
                        res_df = wj_items_df_3.query('ID == %s' % int(_answer)).query(
                            'WJ_LIST_CONTENT_ID == %s' % list_content_id)
                        if _answer == "292":
                            print("res_df", res_df)
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
                            if row.get("ID") == "292":
                                print("target_table2_df", target_table2_df)
                            if len(target_table2_df) > 1:
                                print("映射关系表中不能存在相同设置QUESTION_CLASS=2  real_list_content_id:%s real_answer:%s " % (
                                    real_list_content_id, real_answer))
                                # break
                            if len(target_table2_df) < 0:
                                print("无法获取映射表中的映射关系QUESTION_CLASS=2 real_list_content_id %s real_answer:%s " % (
                                    real_list_content_id, real_answer))
                                # break
                            for index1, row1 in target_table2_df.iterrows():  # 在映射表中获取到映射关系
                                if int(row1["CLASSID"]) == 1:
                                    TARGET_TABLE = row1.get("TARGET_TABLE")
                                    if TARGET_TABLE == "personal_info":  # 到个人信息库
                                        personal_info_tmp.setdefault(row1["TARGET_FILED_ID"],
                                                                     row1["TARGET_FILED_VALUE"])
                                        # tj_record_tmp.setdefault("DABH", DABH)
                                    if TARGET_TABLE == "tj_record":  # 到体检库
                                        # tj_record_tmp.setdefault("DABH", DABH)
                                        tj_record_tmp.setdefault(row1["TARGET_FILED_ID"], answer)
                                elif int(row1["CLASSID"]) == 2:
                                    tj_result_tmp.setdefault(row1["TARGET_FILED_ID"], row1["TARGET_FILED_VALUE"])
                                    if not tj_result_tmp.get("DICT_CODE"):
                                        tj_result_tmp["DICT_CODE"] = row1["TARGET_FILED_CODE"]
                                break  # 只取第一条
            if tj_result_tmp:
                tj_result_tmp["WJ_ANSWER_MASTER_ID"] = wj_answer_master_row["ID"]
                new2 = pd.DataFrame(tj_result_tmp, index=[1])
                result_info_df = result_info_df.append(new2, ignore_index=True)
            if personal_info_tmp:
                personal_info_tmp.setdefault("OLD_ID", wj_answer_master_row["ID"])


def get_answer_map(wj_items_df_3,map_table_df,answer,list_content_id,question_class):
    personal_info_tmp = {}
    tj_record_tmp = {}
    tj_result_tmp = {}
    ret_tmp = {}
    if not answer:
        return None
    items_df = wj_items_df_3.query('ID == %s' % answer).query(
        'WJ_LIST_CONTENT_ID == %s' % list_content_id)
    if len(items_df) != 1:
        return None
    # print("items_df",items_df)
    for index,items_row in items_df.iterrows():
        real_list_content_id = items_row.get("WJ_LIST_CONTENT_ID")
        real_answer = items_row.get("WJ_ITEMS_CONTENT_ID")
        WJ_ITEMNAME = items_row.get("WJ_ITEMNAME")
    # print("real_list_content_id",real_list_content_id)
    # print("real_answer",real_answer)
    #     print("WJ_ITEMNAME",WJ_ITEMNAME)
        if not all([real_list_content_id, real_answer,WJ_ITEMNAME]):
            return None
        map_table = map_table_df.query('TYPEID == %s' % int(question_class)).query(
            'SOURCE_FILED_ID == "%s"' % real_list_content_id).query(
            'SOURCE_FIELD_CODE == "%s"' % real_answer)
        if len(map_table) != 1:
            return None
        # print("map_table", map_table)
        for index,map_table_row in map_table.iterrows():
            class_id = map_table_row.get("CLASSID")
            type_id = map_table_row.get("TYPEID")
            TARGET_TABLE = map_table_row.get("TARGET_TABLE")
            # print("class_id",class_id)
            # print("TARGET_TABLE",TARGET_TABLE,type(TARGET_TABLE))
            TARGET_FILED_ID = map_table_row.get("TARGET_FILED_ID")
            TARGET_FILED_VALUE = map_table_row.get("TARGET_FILED_VALUE")
            TARGET_FILED_CODE = map_table_row.get("TARGET_FILED_CODE")
            # if not all([class_id,TARGET_TABLE,TARGET_FILED_ID,TARGET_FILED_VALUE,TARGET_FILED_CODE]):
            #     return None
            # if int(class_id) == 1:
            # if TARGET_TABLE == "personal_info":  # 到个人信息库
            #     ret_tmp.setdefault(TARGET_FILED_ID,TARGET_FILED_VALUE)
            # if TARGET_TABLE == "tj_record":  # 到体检库主表
            #     ret_tmp.setdefault(TARGET_FILED_ID, answer)
            # elif int(class_id) == 2:  # 体检库从表信息
            if TARGET_TABLE == "tj_result":
                if not all([TARGET_FILED_CODE,TARGET_FILED_ID,class_id]):
                    return None
                ret_tmp.setdefault("DICT_CODE", TARGET_FILED_CODE)
                ret_tmp.setdefault(TARGET_FILED_ID, TARGET_FILED_VALUE)
                if type_id == 1:
                    ret_tmp.setdefault(TARGET_FILED_ID, answer)
                    print("WJ_ITEMNAME", WJ_ITEMNAME)
                if type_id == 2:
                    ret_tmp.setdefault(TARGET_FILED_ID, TARGET_FILED_VALUE)

    return ret_tmp


if __name__ == '__main__':
    # from ETLSchedule.ETL.extracter.extract import Extract
    # from ETLSchedule.ETL.transformer.trannform import BaseTransForm
    # print(type(pickle.dumps(Extract)))
    # extract = Extract()
    # transfrom = BaseTransForm()
    # print(dir(extract))
    # print(list(filter(lambda x:not x.startswith("__") and callable(getattr(extract,x)),dir(extract))))
    # def s():
    #     print("ok")
    import inspect
    # print(inspect.getargspec(s))
    # print(extract.__dict__)
    # for name, value in vars(extract).items():
    #     print(name,value)
    # print(dir(transfrom))
    # print((list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and getattr(transfrom, m),
    #                         dir(transfrom)))))
    # for name,value in vars(transfrom).items():
    #     print(name,value)
    # print(vars(transfrom).keys())
    # print(inspect.getargspec(extract.read_file))
    # from jwt import jwt
    import jwt
    from jwt.jwk import OctetJWK
    # headers = {
    #     'alg': "HS256",  # 声明所使用的算法
    # }

    # token_dict = {"tel":"13421144083"}  # eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0ZWwiOiAiMTM0MjExNDQwODMifQ.1gzfHJ-VaPSHyZQDIwsG4-SpxgQ5DgFiKMrewH9Qph0
    # token_dict = {"tel":"13421144081"}                       # eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0ZWwiOiAiMTM0MjExNDQwODEifQ.Rbw5LygJRN9adoll-i5c32QtaJk3uFNUcCv660GWh5Q
    # token_dict = {"tel":"13421144081sfwefsfvwfvsbvwfgqwfw"}                        # eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0ZWwiOiAiMTM0MjExNDQwODFzZndlZnNmdndmdnNidndmZ3F3ZncifQ.BdF_-526Tr7Lpg2FruyCPEOvuxaldaTPR046DlHNvlU
    # token_dict = {"tel":"13421144081sfwefsfvwfvsbvwfgqwfw","name":"fasfwerfafsf"}  # eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0ZWwiOiAiMTM0MjExNDQwODFzZndlZnNmdndmdnNidndmZ3F3ZnciLCAibmFtZSI6ICJmYXNmd2VyZmFmc2YifQ.89_oLovzkgH5Ds3Pfq_uteC0n262BaFD-ligS33opYw
    # "zhananbudanchou1234678",  # 进行加密签名的密钥
    # algorithm = "HS256",  # 指明签名算法方式, 默认也是HS256
    # headers = headers  # json web token 数据结构包含两部分, payload(有效载体), headers(标头) # payload, 有效载体
    # a = jwt.JWT()
    # key = OctetJWK(key=b'asdfwetfasfwetwqtgweffsfwqfg')
    # jwt_token = a.encode(token_dict,key)
    # data_dict = a.decode(jwt_token,key=key)
    # print(jwt_token)
    # print(data_dict)
    # tel_name_openid_alias
    # str = "13421144083_longsee_Xunnadg342zDdaweDFEW_LLs"
    # token_dict = {"tel": "13421144083"}
    # print(a.encode(str,key))

    # import difflib
    #
    # def get_equal_rate(str1, str2):
    #     return difflib.SequenceMatcher(None, str1, str2).quick_ratio()

    # print(get_equal_rate('eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0ZWwiOiAiMTM0MjExNDQwODMifQ.1gzfHJ-VaPSHyZQDIwsG4-SpxgQ5DgFiKMrewH9Qph0','eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ0ZWwiOiAiMTM0MjExNDQwODEifQ.Rbw5LygJRN9adoll-i5c32QtaJk3uFNUcCv660GWh5Q'))

    # import csv, re
    # from collections import namedtuple
    #
    # with open(r'C:\Users\Administrator.L129LWCESYTQ558\AppData\Roaming\Microsoft\Windows\Network Shortcuts\xxx.xlsx') as f:
    #     f_csv = csv.reader(f)
    #     headers = [re.sub('[^A-Za-z]', 'Q', h) for h in next(f_csv)]
    #     print(headers)
    #     Row = namedtuple('d', headers)
    #     for r in f_csv:
    #         row = Row(*r)
    #         print(row)

    # import filetype
    #
    # kind = filetype.guess(r'C:\Users\Administrator.L129LWCESYTQ558\AppData\Roaming\Microsoft\Windows\Network Shortcuts\xxx.xlsx')
    # if kind is None:
    #     print('Cannot guess file type!')
    # if kind:
    #     print('File extension: %s' % kind.extension)
    #     print('File MIME type: %s' % kind.mime)

    # coding:utf-8
    # WinSonZhao
    # import os

    # 支持文件类型
    # 用16进制字符串的目的是可以知道文件头是多少字节
    # 各种文件头的长度不一样，少半2字符，长则8字符
    # def typeList():
    #     print('获取文件格式十六进制码表……')
    #     return { "68746D6C3E": 'html',
    #              "d0cf11e0a1b11ae10000": 'xls',
    #              "44656C69766572792D64": 'eml',
    #              'ffd8ffe000104a464946': 'jpg',
    #              '89504e470d0a1a0a0000': 'png',
    #              '47494638396126026f01': 'gif',
    #              '49492a00227105008037': 'tif',
    #              '424d228c010000000000': 'bmp',
    #              '424d8240090000000000': 'bmp',
    #              '424d8e1b030000000000': 'bmp',
    #              '41433130313500000000': 'dwg',
    #              '3c21444f435459504520': 'html',
    #             '3c21646f637479706520': 'htm',
    #             '48544d4c207b0d0a0942': 'css',
    #             '696b2e71623d696b2e71': 'js',
    #             '7b5c727466315c616e73': 'rtf',
    #             '38425053000100000000': 'psd',
    #             '46726f6d3a203d3f6762': 'eml',
    #             'd0cf11e0a1b11ae10000': 'doc',
    #             'd0cf11e0a1b11ae10000': 'vsd',
    #             '5374616E64617264204A': 'mdb',
    #             '252150532D41646F6265': 'ps',
    #             '255044462d312e350d0a': 'pdf',
    #             '2e524d46000000120001': 'rmvb',
    #             '464c5601050000000900': 'flv',
    #             '00000020667479706d70': 'mp4',
    #             '49443303000000002176': 'mp3',
    #             '000001ba210001000180': 'mpg',
    #             '3026b2758e66cf11a6d9': 'wmv',
    #             '52494646e27807005741': 'wav',
    #             '52494646d07d60074156': 'avi',
    #             '4d546864000000060001': 'mid',
    #             '504b0304140000080044': 'zip',
    #             '504b03040a0000080000': 'zip',
    #             '504b03040a0000000000': 'zip',
    #             '526172211a0700cf9073': 'rar',
    #             '235468697320636f6e66': 'ini',
    #             '504b03040a0000000000': 'jar',
    #             '4d5a9000030000000400': 'exe',
    #             '3c25402070616765206c': 'jsp',
    #             '4d616e69666573742d56': 'mf',
    #             '3c3f786d6c2076657273': 'xml',
    #             '494e5345525420494e54': 'sql',
    #             '7061636b616765207765': 'java',
    #             '406563686f206f66660d': 'bat',
    #             '1f8b0800000000000000': 'gz',
    #             '6c6f67346a2e726f6f74': 'properties',
    #             'cafebabe0000002e0041': 'class',
    #             '49545346030000006000': 'chm',
    #             '04000000010000001300': 'mxp',
    #             '504b0304140006000800': 'docx',
    #             'd0cf11e0a1b11ae10000': 'wps',
    #             '6431303a637265617465': 'torrent',}
    #
    # # 字节码转16进制字符串
    # def bytes2hex(bytes):
    #     print('关键码转码……')
    #     num = len(bytes)
    #     hexstr = u""
    #     for i in range(num):
    #         t = u"%x" % bytes[i]
    #         if len(t) % 2:
    #             hexstr += u"0"
    #             hexstr += t
    #     return hexstr.upper()
    #
    # # 获取文件类型
    # def filetype(filename):
    #     print('读文件二进制码中……')
    #     binfile = open(filename, 'rb')  # 必需二制字读取
    #     print('提取关键码……')
    #     bins = binfile.read(20)  # 提取20个字符
    #     binfile.close()  # 关闭文件流
    #     bins = bytes2hex(bins)  # 转码
    #     bins = bins.lower()  # 小写
    #     print(bins)
    #     tl = typeList() # 文件类型
    #     ftype = 'unknown'
    #     print('关键码比对中……')
    #     for hcode in tl.keys():
    #         lens = len(hcode)  # 需要的长度
    #         if bins[0:lens] == hcode:
    #             ftype = tl[hcode]
    #         break
    #     if ftype == 'unknown':  # 全码未找到，优化处理，码表取5位验证
    #         bins = bins[0:5]
    #         for hcode in tl.keys():
    #             if len(hcode) > 5 and bins == hcode[0:5]:
    #                 ftype = tl[hcode]
    #             break
    #     return ftype
    #
    # # 文件扫描，如果是目录，就将遍历文件，是文件就判断文件类型
    # def file_scanner(path):
    #     if type(path) != type('a'):  # 判断是否为字符串
    #         print('抱歉，你输入的不是一个字符串路径！')
    #     elif path.strip() == '': # 将两头的空格移除
    #         print('输入的路径为空！')
    #     elif not os.path.exists(path):
    #         print('输入的路径不存在！')
    #     elif os.path.isfile(path):
    #         print('输入的路径指向的是文件，验证文件类型……')
    #         if path.rfind('.') > 0:
    #             print('文件名:', os.path.split(path)[1])
    #         else:
    #             print('文件名中没有找到格式')
    #         path = filetype(path)
    #         print('解析文件判断格式：' + path)
    #     elif os.path.isdir(path):
    #         print('输入的路径指向的是目录，开始遍历文件')
    #         for p, d, fs in os.walk(path):
    #             print(os.path.split(p))
    #             for n in fs:
    #                 n = n.split('.')
    #                 print('\t' + n[0] + '\t' + n[1])
# if __name__ == '__main__':
#     print('WinSonZhao，欢迎你使用文件扫描工具……')
#     path = input('请输入要扫描的文件夹路径：')
#     path = r'C:\Users\Administrator.L129LWCESYTQ558\AppData\Roaming\Microsoft\Windows\Network Shortcuts\病原微生物检测-48h_20200608122000.csv'
#     file_scanner(path)
#     print('扫描结束！')
#     his_list = ["64a5e7b2-8098-11e6-a1ab-fa163ef5a45d"]
#     args = ','.join(['%s'] * len(his_list))
#     a = "select * from s where 1 in (%s)" % (args)
#     a = a % tuple(his_list)
#     print(a)

    #
    import re
    a = "0132512343"
    b = '["天津市","市辖区"]'
    c = "NAN"
    d = 1
    e = "天津市"
    f = '["261","234","242"]'
    g = "1"
    h = "362,361,234,5635,2341,3453"
    i = "04"
    # result = h.split(",")
    # result = g.split(",")
    # if result:
    #     print("result",result)
    # else:
    # res = re.match(r"^(?P<str>[0-9]\d*,[0-9]\d*)|(?P<num>[0-9]\d*)|(?P<list>\[.+?\])$",a)  # (?P<num>[0-9]\d*)
    res = re.match(r"^(?P<num>[0-9]\d*$)|(?P<str>(\d+,)*\d+$)|(?P<list>\[.+?\]$)", h)
    # res = re.match(r"^(?P<str>[0-9]\d*,[0-9]\d*)$",h)  # (?P<num>[0-9]\d*)
    # res = re.match(r"(?P<num>[0-9]\d*)|(?P<list>\[.+?\])|(?P=num),(?P=num)$",h)
    print('res',res)
    if res:
        print(res.lastgroup)
        if res.lastgroup == "num":
            ret = int(res.group(0))
        elif res.lastgroup == "list":
            ret = eval(res.group(0))
        elif res.lastgroup == "str":
            ret = "[" + str(res.group(0)) + "]"
            ret = eval(ret)
        else:
            ret = None
        print("ret",ret)
    # def 大乐透(data):
    #     print(data)
    # 大乐透('sss')
    # 我 = '事实上'
    # print("我",我)
    # class 中国人(object):
    #     def __init__(self):
    #         self.性别 = "男"
    #         self.名字 = "林文森"
    #
    #     def 吃饭(self):
    #         print(self.名字 + "正在吃饭")
    # 林文森 = 中国人()
    # 林文森.吃饭()
    # print(int(a))
    # print(a.zfill(4))
    # print(a.format("1"))

    # import pandas as pd
    # from ETLSchedule.ETL.extracter.extract import Extract
    # pd.set_option('display.max_rows', None)
    # extract = Extract()
    # source = '''{"connect": {"database": "db_mid_bigdata", "ip": "192.168.1.100", "password": "longseeuser01", "port": 3306,
    #     "user": "user01"}, "id": 2, "sql": "select * from test_apply where WJID != 'NULL';", "type": 1}'''
    # source = eval(source)
    # connect = source.get('connect')
    # sql = source.get('sql')
    # data_frame = extract.read_mysql(sql, connect)
    # print("data_frame",data_frame)
    # print("data_frame_describe",data_frame.describe())

    # import pickle
    # # def sayhi(name):
    # #     print("hello,", name)
    # def sayhi(name):
    #     pass
    #
    #
    # info = {'name': 'alex',
    #         'age': 22,
    #         'func': sayhi}
    # f = open("test.text", "wb")
    # pickle.dump(info, f)
    # f.close()
    # f = open("test.text","rb")
    # info = pickle.load(f)
    # print(info)
    # f.close()
    # sayhi = info.get("func")
    # sayhi("sdfasd")

    # def test1(age:int,name:str):
    #     print('age',age)
    #     print('name',name)
    # # test1(18,'22')
    # test1("18",22)

    # import os,sys,datetime
    #
    # file_path = os.path.join(sys.path[0], "logs",
    #                          "%s.log" % datetime.datetime.strftime(datetime.datetime.now(), "%Y_%m_%d"))
    # f = open(file_path,"w")
    # f.close()

    # if not os.path.exists(file_path):
        # os.system(r"touch {}".format(filename))
        # os.makedirs(filename)
        # os.makedirs(filename)
        # os.system(r"touch")
        # print(file_path)
        # os.system(r"touch {}".format(file_path))




