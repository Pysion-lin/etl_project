
def parse_parameter(type_id,parameter):
    if type_id == 1:
        user = parameter["user"]
        password = parameter["password"]
        ip = parameter["ip"]
        port = parameter["port"]
        database = parameter["database"]
        table = parameter["table"]
    elif type_id == 2:
        user = parameter["user"]
        password = parameter["password"]
        ip = parameter["ip"]
        database = parameter["database"]
        table = parameter["table"]


def get_str_btw(s, f, b):
    par = s.partition(f)
    return [str(m).strip() for m in str((par[2].partition(b))[0]).split(",")]


def get_str_btw1(s,f,b):
    par = s.partition(f)
    return [str(m).strip() for m in str((par[2].partition(b))[0]).split(" ")]