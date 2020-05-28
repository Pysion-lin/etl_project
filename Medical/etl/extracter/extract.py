import pandas as pd
import json,requests,traceback
from Medical.etl.models.models import TBEmployeeModel


class Extract(object):
    '''数据抽取基类'''
    def __init__(self):
        self.file_path = None

    def read_file(self,file_path=None,header=None,sep=" = "):
        '''读取指定文件路径的方法,file_path:文件路径,header:是否包含头,sep:数据分割符'''
        if not file_path:
            raise ValueError("请提供文件路径")
        try:
            ret = pd.read_table(file_path,header=header,sep=sep)
        except Exception as e:
           raise ValueError(e.__str__())
        return ret

    # def csv_extract(self,file_path,header=None,sep=" "):
    #     if not file_path:
    #         raise ValueError("请提供文件路径")
    #     try:
    #         ret = pd.read_csv(file_path, header=header, sep=sep)
    #     except Exception as e:
    #         raise ValueError(e.__str__())
    #     return ret
    #
    # def ftp_extract(self):
    #     print("i am ftp")
    #     pass
    #
    # def mysql_extract(self):
    #     print("i am mysql")
    #     pass
    #
    # def url_extract(self):
    #     print("i am url")
    #     pass
    #
    # def read_json(self,url):
    #     if not url:
    #         re = requests.get("http://192.168.1.103:8002/kit/forwardinfo/?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpZCI6NSwiZGVwYXJ0bWVudCI6eyJpZCI6MSwibmFtZSI6Ilx1NTkyN1x1NjU3MFx1NjM2ZVx1NGUyZFx1NWZjMyJ9LCJ0ZWwiOiIxODgxNDA5NDA2MCIsIm5hbWUiOiJcdTY3OTdcdTY1ODdcdTY4ZWUiLCJuYW1lX2NvZGUiOiJsb25nc2VlIiwicGFzc3dvcmQiOiJMbDEyMzQ1Njc4OSIsImlzX3N1cGVyIjoxLCJpc19hdWRpdCI6MSwiY3JlYXRlX3RpbWUiOiIyMDIwLTA0LTI4VDAwOjAwOjAwIiwidXBkYXRlX3RpbWUiOiIyMDIwLTA1LTE2VDE2OjM2OjIwIiwiZXhwaXJlcyI6IjIwMjAtMDUtMjIgMTc6NTE6MTgifQ.gwQm_CG3Zlrv3wp2jPDvi5_VMz8z4I5MymCJ7K5A-d8&instrument_id=6")
    #     else:
    #         re = requests.get(url)
    #     try:
    #         json_data = json.dumps(eval(re.text))
    #     except Exception as e:
    #         raise ValueError("通过url获取的的数据不是json格式")
    #     df = pd.read_json(json_data)
    #     return df

    def read_mysql(self,sql):
        '''读取指定SQLserver数据库的数据,sql:查询表的语句 如:select * from TB_BDM_Employee'''
        from Medical.etl.models import engine_SQL_Server
        sql = "select * from TB_BDM_Employee"
        try:
            with engine_SQL_Server.connect() as con, con.begin():
                df = pd.read_sql(sql, con)  # 获取数据
                con.close()
        except Exception as e:
            df = None
        print("read_mysql:",df)
        return df
        # try:
        #     result = engine_SQL_Server.query(TBEmployeeModel).all()
        #     # context.log.info("the type is {}".format(type(result)))
        #     # context.log.info("result is {}".format(result))
        #     engine_SQL_Server.close()
        #     return result
        # except Exception as e:
        #     traceback.print_exc()
        #     # context.log.error("error info: {}".format(e.__str__()))
        # return result

    def methods__(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))

