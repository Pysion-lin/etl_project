from . import BaseTask
from settings.dev import BASE_DIR

import os


class LogTask(BaseTask):
    def __init__(self):
        super().__init__()

    def transfrom_data(self):
        path = os.path.join(BASE_DIR,"tmp","NASA_access_log_Aug95.txt")
        data = self.read_file(path)
        df = self.split_data(data)
        if df is not None:
            return df
        else:
            return None

    def run(self):
        model = self.transfrom_data()
        print("model:",model)
        self.load_data(model)
        a = {}





