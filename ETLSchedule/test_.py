

if __name__ == '__main__':
    from ETLSchedul import Extract
    # print(type(pickle.dumps(Extract)))
    extract = Extract()
    # print(dir(extract))
    # print(list(filter(lambda x:not x.startswith("__") and callable(getattr(extract,x)),dir(extract))))
    def s():
        print("ok")
    import inspect
    # print(inspect.getargspec(s))
    print(inspect.getargspec(extract.read_file))


