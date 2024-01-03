class DataMissingError(Exception):
    def __init__(self, message="数据缺失异常"):
        self.message = message
        super().__init__(self.message)
