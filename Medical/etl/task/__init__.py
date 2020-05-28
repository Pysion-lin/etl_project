from Medical.etl.transformer import BaseTransForm
from Medical.etl.loader.loader import LoadData
from Medical.etl.extracter.extract import Extract


class BaseTask(BaseTransForm,LoadData,Extract):
    def __init__(self):
        super().__init__()

    def transfrom_data(self):
       pass

    def run(self):
        pass

