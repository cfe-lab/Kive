class SandboxActiveException(Exception):
    """
    Exception raised when attempting to perform garbage collection on
    a sandbox that is still active.
    """
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg