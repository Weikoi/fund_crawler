# @Time : 2021/2/4 21:47 
# @Author : Huang Zengrui
# @Email : huangzengrui@yahoo.com
# @Desc: 自定义异常类


class ExternalException(Exception):
    """外部原因导致的异常"""
    pass


class InternalException(Exception):
    """系统原因导致的异常"""
    pass


class NetworkException(ExternalException):
    """网络原因导致的异常"""
    pass


class LimitedException(ExternalException):
    """触发反爬机制导致的异常"""
    pass


class TimeoutException(InternalException):
    """超时导致的异常"""
    pass
