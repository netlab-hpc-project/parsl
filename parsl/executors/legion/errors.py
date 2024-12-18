from parsl.app.errors import AppException
from parsl.errors import ParslError


class LegionTaskFailure(AppException):
    """A failure executing a task in Legion

    Contains:
    reason(string)
    status(int)
    """

    def __init__(self, reason: str, status: int):
        self.reason = reason
        self.status = status
        
        

class LegionRuntimeFailure(ParslError):
    """A failure in the Legion Runtime that prevented the task to be
    executed.
    """
    pass

