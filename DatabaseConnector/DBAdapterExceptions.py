class DBAdapterError(Exception):
    """Exception raised for errors in the input.
    """

    def __init__(self, code = 0, msg = ''):
        self.code = code
        self.msg = msg
        
    def __str__(self):
        return repr(self.msg)