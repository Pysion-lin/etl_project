#


class BaseTransForm(object):
    map_path = {
        "file":"path",
        "api":"url",
        "database":"url+database"
    }

    def __init__(self):
        # super().__init__()
        print("baseTransform __init__")
        pass

    # def process_nall(self,dataframe):
    #     return pd.DataFrame(dataframe).dropna(inplace=True)
    #
    # def get_data_(self,path):
    #     extract = Extract()
    #     amp = path.get(list(path.keys())[0]) if list(path.keys())[0] in list(self.map_path.keys()) else None
    #     if not amp:
    #         raise ValueError("数据请求获取方式不能存在")
    #     data = extract.read_file(amp)
    #     return data

    # @staticmethod
    # def process2__(x):
    #     split_data = str(x).split(" ")
    #     data = [split_data[0], str(split_data[5]).replace('"',""), split_data[6], split_data[8]]
    #     return data

    def selector(self,dataframe,column):
        '''选择dataframe中的某一列,column是列名'''
        serial = dataframe[column]
        return serial

    def mapping(self,dataframe,column,from_data,to_data):
        '''将某一列的值进行映射,column:列名,from_data:源数据,to_data:目标数据'''
        dataframe["Version"] = dataframe['Version'].map(lambda x: None if type(x) == bytes else x)
        dataframe[column] = dataframe[column].map(lambda x: to_data if x == from_data else x)
        return dataframe

    def split_data(self,dataframe,column,sep):
        '''切分数据 column:字段名,sep:分割符'''
        # data_list = []
        # method_data = dataframe[0].map(self.process2__)
        print("split data finish")
        return dataframe

    # def model(self,data):
    #     return AttrDict(data)

    def methods__(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))


class AttrDict(dict):
    def __init__(self,*args,**kwargs):
        super(AttrDict,self).__init__(*args,**kwargs)
        self.__dict__ = self