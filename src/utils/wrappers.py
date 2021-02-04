#!usr/bin/env python
# -*- coding: utf-8 -*-
# @Time : 2021/2/4 13:51
# @Author : Huang Zengrui huangzengrui@yahoo.com
# @File : wrappers.py 
# @Desc :装饰器工具


import functools
import timeit
from src.utils.log_tools import get_logger
import datetime

logger = get_logger(file_name="Wrappers", logger_name="")


def catch_error(func):
    """用日志记录异常捕捉"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(e)

    return wrapper


def time_log_info(func):
    """用日志记录函数运行时间"""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # 运行前
        start = timeit.default_timer()
        # 运行中
        res = func(*args, **kwargs)
        # 运行后
        time_cost = timeit.default_timer() - start
        logger.warning("执行耗时: {:.8f}s  函数[{}] ".format(time_cost, func.__name__))
        return res

    return wrapper


def lru_cache_until_time(maxsize=128, typed=False, state=None, unhashable='error', expire_time=None):
    """使用LRU加载缓存，并可以指定缓存失效时间"""
    expire_time = expire_time
    today = datetime.date.today()
    expire_dt = datetime.datetime.strptime(expire_time, '%H:%M:%S').replace(year=today.year, month=today.month,
                                                                            day=today.day)
    is_expired = lambda: datetime.datetime.now() >= expire_dt

    class obj(object):
        has_refreshed = False

    o = obj()

    def cache_wrapper(func):
        from fastcache import clru_cache
        cached_func = clru_cache(maxsize, typed, state, unhashable)(func)

        def inner(*args, **kwargs):
            if not o.has_refreshed and is_expired():
                cached_func.cache_clear()
                o.has_refreshed = True

            return cached_func(*args, **kwargs)

        return inner

    return cache_wrapper
