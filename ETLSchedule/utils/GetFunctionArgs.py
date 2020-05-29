import inspect


def get_func_args(name,obj):
    method = getattr(obj, name)
    return [x for x in inspect.getargspec(method).args if x != "self" and x != "dataframe"]