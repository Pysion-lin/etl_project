from ETL.transformer.trannform import BaseTransForm
from ETL.loader.loader import LoadData
from ETL.extracter.extract import Extract


# 这是一个任务工厂类
class BaseTask(BaseTransForm,LoadData,Extract):
    def __init__(self):
        super().__init__()

    def factory(self):
        pass

