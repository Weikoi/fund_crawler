from src.spider.crawler import *
import datetime
from src.utils.log_tools import get_logger

logger = get_logger(file_name="spider", logger_name="main")

if __name__ == '__main__':
    # ============>>>>>>>>>每日更新：
    logger.info("====>>>>>开始执行爬虫...")
    # 获取公司列表
    # get_company_list()

    # 获取基金列表
    # get_fund_list()

    # 基金信息下载与处理
    # get_category_data()

    # # std 和夏普比率信息下载
    download_f10_ts_data()
    # # 基金经理信息下载
    # download_manager_info()
    #
    #
    # # std 和夏普比率信息处理
    solve_f10_data()
    # # 基金经理信息处理
    # solve_manager_info()
    # # pingzhong data 处理
    # solve_crawler3()
