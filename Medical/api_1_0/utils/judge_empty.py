

def Judge_Empty(type,data):
    # try:
    if not data:
        raise ValueError("参数不能为空")
    if type == 1:  # 列表类型
        for list_data in data:
            print("list_data",list_data)
            for key in list(list_data.keys()):
                if not list_data.get(key):
                    raise ValueError("参数不能为空")
    elif type == 2:  # 字典类型
        print(data)
        for key in list(data.keys()):
            if not data.get(key):
                raise ValueError("参数不能为空")

    # except Exception as e:
    #     raise ValueError("数据判空出错 {}".format(e.__str__()))