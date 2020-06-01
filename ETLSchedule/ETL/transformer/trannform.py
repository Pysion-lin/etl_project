#


class BaseTransForm(object):

    def __init__(self):
        self.selector_dict = {"module_id":1,"primary_key":False}
        self.mapping_dict = {"module_id":1,"primary_key":False}
        self.split_data_dict = {"module_id":2,"primary_key":False}
        self.select_primary_key_dict = {"module_id":3,"primary_key":True}
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

    def select_primary_key(self):
        '''选择字段作为装载数据时的判断是否重复的依据(每个任务必填),此处仅提供可视化选择对应的参数列表,实际处理在loader模块'''
        pass

    def xx(self):
        pass

    # def model(self,data):
    #     return AttrDict(data)
    def methods__(self):
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                            dir(self))))

        # return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and getattr(self, m),
        #                     dir(self))))


class AttrDict(dict):
    def __init__(self,*args,**kwargs):
        super(AttrDict,self).__init__(*args,**kwargs)
        self.__dict__ = self