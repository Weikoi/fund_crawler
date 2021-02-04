#!usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time :2020/12/29
@Author:huangzengrui@htsc.com
@desc: 日志包装类及配置接口
"""

__all__ = ["LogFactory", "get_logger"]

import logging
import logging.config
import logging.handlers

from datetime import datetime
import os
import six


class _InfoFilter(logging.Filter):
    def filter(self, record):
        """only use INFO

        筛选, 只需要 INFO 级别的log

        :param record:
        :return:
        """
        if logging.INFO <= record.levelno:
            # 已经是INFO级别了
            # 然后利用父类, 返回 1
            return super().filter(record)
        else:
            return 0


class _DebugFilter(logging.Filter):
    def filter(self, record):
        """only use INFO

        筛选, 只需要 debug 级别的log

        :param record:
        :return:
        """
        if logging.DEBUG == record.levelno:
            # 已经是INFO级别了
            # 然后利用父类, 返回 1
            return super().filter(record)
        else:
            return 0


# @six.add_metaclass(metaclass=Singleton)
class LogFactory(object):
    # 每个日志文件，使用 2GB
    _SINGLE_FILE_MAX_BYTES = 2 * 1024 * 1024 * 1024
    # 轮转数量是 20 个
    _BACKUP_COUNT = 60

    def __init__(self, basename):
        # 基于 dictConfig，做再次封装
        self._log_config_dict = {
            'version': 1,

            'disable_existing_loggers': False,

            'formatters': {
                # 开发环境下的配置
                'dev': {
                    'class': 'logging.Formatter',
                    'format': '[%(asctime)s] - [%(levelname)-7s] - [pid: %(process)d] -'
                              ' [%(filename)s %(lineno)s %(funcName)s] - [%(message)s]'
                },
                # 生产环境下的格式(越详细越好)
                'prod': {
                    'class': 'logging.Formatter',
                    'format': ('%(asctime)s-%(levelname)s-[pid: %(process)d]-'
                               '[%(filename)s %(lineno)s %(funcName)s] %(message)s')
                }

                # 使用UTC时间!!!

            },

            # 针对 LogRecord 的筛选器
            'filters': {
                'info_filter': {
                    '()': _InfoFilter,

                },
                'debug_filter': {
                    '()': _DebugFilter,
                }
            },

            # 处理器(被loggers使用)
            'handlers': {
                'console': {  # 按理来说, console只收集ERROR级别的较好
                    'class': 'logging.StreamHandler',
                    'level': 'WARNING',
                    'formatter': 'dev'
                },

                'file': {
                    'level': 'INFO',
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': self._get_filename(basename=basename, log_level='info'),
                    'maxBytes': self._SINGLE_FILE_MAX_BYTES,  # 2GB
                    'encoding': 'UTF-8',
                    'backupCount': self._BACKUP_COUNT,
                    'formatter': 'dev',
                    'delay': True,
                    'filters': ['info_filter', ]  # only INFO, no ERROR
                },
                'file_error': {
                    'level': 'ERROR',
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': self._get_filename(basename=basename, log_level='error'),
                    'maxBytes': self._SINGLE_FILE_MAX_BYTES,  # 2GB
                    'encoding': 'UTF-8',
                    'backupCount': self._BACKUP_COUNT,
                    'formatter': 'dev',
                    'delay': True,
                },
                'file_debug': {
                    'level': 'DEBUG',
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': self._get_filename(basename=basename, log_level='debug'),
                    'maxBytes': self._SINGLE_FILE_MAX_BYTES,  # 2GB
                    'encoding': 'UTF-8',
                    'backupCount': self._BACKUP_COUNT,
                    'formatter': 'dev',
                    'delay': True,
                    'filters': ['debug_filter', ]  # only DEBUG
                },

            },

            # 真正的logger(by name), 可以有丰富的配置
            'loggers': {
                # ''代表默认设置
                '': {
                    # 输送到3个handler，它们的作用分别如下
                    #   1. console：控制台输出，方便我们直接查看，只记录ERROR以上的日志就好
                    #   2. file： 输送到文件，记录INFO以上的日志，方便日后回溯分析
                    #   3. file_error：输送到文件（与上面相同），但是只记录ERROR级别以上的日志，方便研发人员排错
                    'handlers': ['console', 'file', 'file_error', 'file_debug'],
                    'level': 'DEBUG'
                },
            },
        }

        logging.config.dictConfig(self._log_config_dict)

    @staticmethod
    def _get_filename(*, basename='app.log', log_level='info'):
        date_str = datetime.today().strftime('%Y%m%d')
        return ''.join((
            basename, '-', date_str, '-', log_level, '.log'))

    @classmethod
    def get_logger(cls, logger_name):
        return logging.getLogger(logger_name)


def get_logger(file_name=None, logger_name=None):
    if file_name is None:
        file_name = "spider"
    if logger_name is None:
        logger_name = "spider"
    return LogFactory(file_name).get_logger(logger_name)


if __name__ == '__main__':
    # papertrade_shedule_task_2020_12_01.log, papertrade_api_2020_12_01.log ??
    SAMPLE_LOGGER = LogFactory('my_demo_log').get_logger(__name__)
    SAMPLE_LOGGER.debug("this is debug")
    SAMPLE_LOGGER.info("this is info")
    SAMPLE_LOGGER.warning("this is warning")
    SAMPLE_LOGGER.error("this is error")
    # SAMPLE_LOGGER.critical("this is critical")

    import time


    def demofunc():
        a = 0
        while a < 100000:
            a += 1
            time.sleep(2)
            SAMPLE_LOGGER.info("{} demo info".format(a))

    demofunc()
