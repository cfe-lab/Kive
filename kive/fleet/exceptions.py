class SandboxActiveException(Exception):
    """
    Exception raised when attempting to perform garbage collection on
    a sandbox that is still active.
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class RTPNotFinished(Exception):
    """
    Exception raised when attempting to remove anything that affects an incomplete Run.
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg