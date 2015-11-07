class StopExecution(Exception):
    """
    Exception raised when a Run has been stopped.
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg