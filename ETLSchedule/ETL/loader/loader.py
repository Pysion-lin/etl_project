from ETLSchedule.models import engine


class LoadData(object):
    def __init__(self):
        print("loadData __init__")
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
    def save_data(self,dataframe,tb="tb_bdm_employee"):
        '''将dataframe的数据装载到mysql数据库中,tb是表名'''
        dataframe.to_sql(tb, engine, if_exists='replace', index=False)


    # def load_data(self, dataframe):
    #     print("dataframe:", dataframe[1])
    #     # data_result = dataframe.map(self.save_data)
    #     # data_result = dataframe.applay(self.save_data)
    #     print("start load data")

    def methods__(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))