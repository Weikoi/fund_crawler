#!usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************
# @Time : 2021/2/24 12:47
# @Author : Huang Zengrui
# @Email : huangzengrui@yahoo.com
# @Desc :
# **************************************************

from src.spider.crawler import *
from src.spider.Data_process import *
import datetime
from src.utils.log_tools import get_logger

logger = get_logger(file_name="spider", logger_name="main")

if __name__ == '__main__':
    # ============>>>>>>>>>每日执行：
    FundSpider("once").process_special_data()
