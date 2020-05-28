import inspect


class Verify(object):
    def __init__(self):
        pass

    def separate(self,cls): # 将cls中的所有方法拆解
        return (list(filter(lambda m: not m.startswith("__") and not m.endswith("__") and callable(getattr(self, m)),
                     dir(cls))))

    def get_parameter(self,func): # 获取每一个函数的所有参数
        # method = getattr(func, name)
        return [x for x in inspect.getargspec(func).args if x != "self"]

    def verify(self,cls,func,parameter):  # 将传入的对象与基类进行校验,以及校验每一个选择的处理模块的所有参数
        cls_list = self.separate(cls)
        if parameter["name"] not in cls_list:
            raise ValueError("选择的处理方法不存在")
        parameters = self.get_parameter(func)
        for k,v in parameter["args"].items():
            if k not in parameters:
                raise ValueError("输入的参数有误")
        return True

