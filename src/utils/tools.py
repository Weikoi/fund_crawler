#!usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************
# @Time : 2021/2/24 12:47
# @Author : Huang Zengrui
# @Email : huangzengrui@yahoo.com
# @Desc : 通用工具函数
# **************************************************


import sys
import math


def progress_bar(portion, total):
    """
    total 总数据大小，portion 已经传送的数据大小
    :param portion: 已经接收的数据量
    :param total: 总数据量
    :return: 接收数据完成，返回True
    """
    part = total / 50  # 1%数据的大小
    count = math.ceil(portion / part)
    sys.stdout.write('\r')
    sys.stdout.write(('[%-50s]%.2f%%' % (('>' * count), portion / total * 100)))
    sys.stdout.flush()

    if portion >= total:
        sys.stdout.write('\n')
        return True
