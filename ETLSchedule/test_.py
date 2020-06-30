

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
    # import re
    # a = "0132512343"
    # b = '["天津市","市辖区"]'
    # c = "NAN"
    # d = 1
    # e = "天津市"
    # f = '["261"]'
    # g = "1"
    # h = "362,361"
    # result = h.split(",")
    # result = g.split(",")
    # if result:
    #     print("result",result)
    # else:
    #     res = re.match(r"(?P<list>\[.+?\])|(?P<new>(?P<num>[0-9]\d*),(?P=num))",h)  # (?P<num>[0-9]\d*)
    #     # res = re.match(r"(?P<num>[0-9]\d*)|(?P<list>\[.+?\])|(?P=num),(?P=num)$",h)
    #     print('res',res)
    #     if res:
    #         if res.lastgroup == "num":
    #             ret = int(res.group(0))
    #             print(ret,type(ret))
    #         if res.lastgroup == "list":
    #             rex = eval(res.group(0))
    #             print(rex,type(rex))
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

    def test1(age:int,name:str):
        print('age',age)
        print('name',name)
    # test1(18,'22')
    test1("18",22)




