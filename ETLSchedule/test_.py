

if __name__ == '__main__':
    from ETLSchedule.ETL.extracter.extract import Extract
    from ETLSchedule.ETL.transformer.trannform import BaseTransForm
    # print(type(pickle.dumps(Extract)))
    extract = Extract()
    transfrom = BaseTransForm()
    # print(dir(extract))
    # print(list(filter(lambda x:not x.startswith("__") and callable(getattr(extract,x)),dir(extract))))
    def s():
        print("ok")
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
    print(vars(transfrom).keys())
    # print(inspect.getargspec(extract.read_file))


