import requests
import pandas as pd
import re
import sys
import math
import datetime
import time
from src.utils.log_tools import get_logger
# from .spider_exception import *
from src.config.global_config import *
from src.data.db_pool import DBPool
from src.config.db_config import DBConfig

logger = get_logger(file_name="spider", logger_name="main")
if TO_DB:
    pool = DBPool(DBConfig.mysql_url)


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


def get_response(url):
    """
    :param url: 网页URL
    :return: 爬取的文本信息
    """
    try:
        r = requests.get(url)
        r.raise_for_status()
        r.encoding = 'utf-8'
        return r.text
    except Exception as e:
        logger.info('Failed to get response by {}'.format(e, url))
        return ''


def get_company_list(url=None):
    """
    :param url: 基金公司信息的URL
    :return: 将结果存储在当前目录Data/company_list.csv中
    """
    now_timestamp = datetime.datetime.timestamp(datetime.datetime.now())
    url = 'http://fund.eastmoney.com/js/jjjz_gs.js?dt={}'.format(now_timestamp)
    logger.info("开始获取基金公司ID信息...")
    response = get_response(url)
    if response:
        pass
    else:
        logger.info("无法获取基金公司ID信息，URL没有响应...")
        raise NetworkException("无法获取基金公司ID信息，URL没有响应...")
    code_list = []
    name_list = []
    tmp = re.findall(r"(\".*?\")", response)
    total_length = len(tmp)
    for i in range(0, len(tmp)):
        if NEED_SLEEP:
            time.sleep(SLEEP_TIME_MIN)
        logger.info("正在获取第 {} / {} 条基金公司ID信息...".format(i + 1, total_length))
        if i % 2 == 0:
            code_list.append(tmp[i].strip("\""))
        else:
            name_list.append(tmp[i].strip("\""))
    data = {'company_id': code_list, 'company_name': name_list}
    df = pd.DataFrame(data)
    if TO_DB:
        pool.insert_by_df("fund_company_info", df)
    df.to_csv('local_data/company_list.csv', index=False)
    logger.info("基金公司ID写入DB与CSV完成<<<<<====")
    logger.info("基金公司ID信息爬取完成<<<<<====")


def get_fund_list(url=None):
    """
    :param url: 基金概况信息的URL
    :return: 将基金统计信息存入当前目录Data/fund_list.csv中,返回基金代码号列表
    """
    logger.info("开始获取基金基本信息...")
    data = {}
    url = 'http://fund.eastmoney.com/js/fundcode_search.js'
    response = get_response(url)
    code_list = []
    abbreviation_list = []
    name_list = []
    type_list = []
    name_en_list = []
    tmp = re.findall(r"(\".*?\")", response)
    total_length = len(tmp)
    for i in range(0, total_length):
        # if NEED_SLEEP:
        #     time.sleep(SLEEP_TIME_MIN)
        logger.info("正在获取第 {} / {} 条基金基本信息...".format(i + 1, total_length))
        if i % 5 == 0:
            code_list.append(eval(tmp[i]))
        elif i % 5 == 1:
            abbreviation_list.append(eval(tmp[i]))
        elif i % 5 == 2:
            name_list.append(eval(tmp[i]))
        elif i % 5 == 3:
            type_list.append(eval(tmp[i]))
        else:
            name_en_list.append(eval(tmp[i]))
    data['fund_id'] = code_list
    data['fund_name'] = name_list
    data['fund_abbr'] = abbreviation_list
    data['fund_type'] = type_list
    # 基金类型种类 {'其他创新', '分级杠杆', '混合型', 'QDII-指数', '混合-FOF', '联接基金', '理财型', '货币型', '定开债券',
    # '债券型', '股票指数', '股票型', 'ETF-场内', '债券指数', '固定收益', 'QDII', '股票-FOF', 'QDII-ETF'}
    print(set(type_list))
    # data['name_en'] = name_en_list
    df = pd.DataFrame(data)
    df.to_csv('local_data/fund_list.csv')
    if TO_DB:
        pool.insert_by_df("fund_info", df)
    logger.info("基金基本信息写入DB与CSV完成<<<<<====")
    logger.info("基金基本信息爬取完成<<<<<====")
    return code_list


def get_fund_info(code):
    failed_list = []
    data_list = {}
    now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    url = 'http://fund.eastmoney.com/pingzhongdata/' + code + '.js?v={}'.format(now)
    response = get_response(url)
    # 爬取失败等待再次爬取
    if response == '':
        return ''
    else:
        result_str = re.findall(r'var(.*?);', response)
        for i in range(0, len(result_str)):
            tmp = result_str[i].split('=')
            var_name = tmp[0].strip()
            data_list[var_name] = [tmp[1]]
        return data_list


def get_category_data():
    data = pd.read_csv('local_data//fund_list.csv')
    code_list = data['fund_id']
    # fS_name ==
    # fS_code ==
    # fund_sourceRate ==
    # fund_Rate ==
    # fund_minsg ==
    # stockCodes ==
    # zqCodes ==
    # stockCodesNew ==
    # zqCodesNew ==
    # syl_1n ==
    # syl_6y ==
    # syl_3y ==
    # syl_1y ==
    # Data_fundSharesPositions ==
    # Data_netWorthTrend ==
    # Data_ACWorthTrend ==
    # Data_grandTotal ==
    # Data_rateInSimilarType ==
    # Data_rateInSimilarPersent ==
    # Data_fluctuationScale ==
    # Data_holderStructure ==
    # Data_assetAllocation ==
    # Data_performanceEvaluation ==
    # Data_currentFundManager ==
    # Data_buySedemption ==
    # swithSameType ==
    data = {'fS_name': [],
            'fS_code': [],
            'fund_sourceRate': [],
            'fund_Rate': [],
            'fund_minsg': [],
            'stockCodes': [],
            'zqCodes': [],
            'stockCodesNew':[],
            'zqCodesNew':[],
            'syl_1n': [],
            'syl_6y': [],
            'syl_3y': [],
            'syl_1y': [],
            'Data_fundSharesPositions':[],
            'Data_netWorthTrend': [],
            'Data_ACWorthTrend': [],
            'Data_grandTotal': [],
            'Data_rateInSimilarType': [],
            'Data_rateInSimilarPersent': [],
            'Data_fluctuationScale': [],
            'Data_holderStructure': [],
            'Data_assetAllocation': [],
            'Data_performanceEvaluation': [],
            'Data_currentFundManager': [],
            'Data_buySedemption': [],
            'swithSameType': []}

    failed_list = []
    time_s = time.time()
    for i in range(0, len(code_list)):
        if NEED_SLEEP:
            time.sleep(SLEEP_TIME_MIN)
        code = '%06d' % code_list[i]
        progress = i / len(code_list) * 100
        print('\r 爬取' + code + '中，进度', '%.2f' % progress + '% ', end='')
        # progress_bar(i, len(code_list))
        fund_info = get_fund_info(code)
        if fund_info == '':
            failed_list.append(code)
        else:
            for key in data.keys():
                if key in fund_info.keys():
                    if 'Data' not in key and key != 'zqCodes':
                        data[key].append(eval(fund_info[key][0]))
                    else:
                        data[key].append(fund_info[key][0])
                else:
                    data[key].append('')
    df = pd.DataFrame(data)
    df.to_csv('local_data/fund_data_list.csv')
    df_fail = pd.DataFrame(failed_list)
    df_fail.to_csv('local_data/fail.csv')
    logger.info("{}共耗时{:.2f}s, 失败{}个".format("get_fund_info", time.time() - time_s, len(df_fail)))


def download_f10_ts_data():
    data = pd.read_csv('local_data//fund_list.csv')
    code_list = data['fund_id']
    for i in range(0, len(code_list)):
        if NEED_SLEEP:
            time.sleep(SLEEP_TIME_MIN)
        # progress_bar(i, len(code_list))
        name = '%06d' % code_list[i]
        progress = i / len(code_list) * 100
        print('\r 爬取' + name + '中，进度', '%.2f' % progress + '% ', end='')
        url = 'http://fund.eastmoney.com/f10/tsdata_' + name + '.html'
        print(url)
        file_name = 'Data/f10_ts/' + name + '.json'
        response = get_response(url)
        # print(response)


def download_manager_info():
    data = pd.read_csv('Data/instruments_ansi.csv', encoding='ANSI')
    code_list = data['code']
    for i in range(0, len(code_list)):
        progress_bar(i, len(code_list))
        name = '%06d' % code_list[i]
        url = 'http://fundf10.eastmoney.com/jjjl_' + name + '.html'
        file_name = 'Data/managerInfo/' + name + '.json'
        response = get_response(url)
        with open(file_name, 'w', encoding='utf-8') as f:
            print(response, file=f)


def download_risk_info():
    data = pd.read_csv('Data/instruments_ansi.csv')
    code_list = data['code']
    for i in range(0, len(code_list)):
        progress_bar(i, len(code_list))
        name = '%06d' % code_list[i]
        url = 'http://fund.eastmoney.com/' + name + '.html'
        file_name = 'Data/risk/' + name + '.json'
        response = get_response(url)
        with open(file_name, 'w', encoding='utf-8') as f:
            print(response, file=f)


class FundSpider(object):
    # todo OOP重构
    def __init__(self):
        pass

    def run_daily_tasks(self):
        """
        这个函数决定每天什么时刻运行什么函数更新什么数据
        :return:
        """
        pass

    def get_fund_list(self):
        """
        每日更新
        这个函数获取每日最新在市基金id
        :return:
        """
        pass

    def get_fund_company_list(self):
        """
        每日更新
        这个函数获取每日最新基金公司id
        :return:
        """
        pass

    def get_fund_info(self):
        """
        这个函数获取基金详情页面数据
        :return:
        """
        pass

    def get_fund_netval(self, from_init=False):
        """
        每日更新
        这个函数获取基金每日净值数据
        :param from_init:boolean 是否从最开始更新净值，默认否，即指增量更新最新的净值
        :return:
        """


if __name__ == '__main__':
    # download_manager_info()
    # solve_f10_data()
    # solve_fund_info()
    # download_risk_info()
    url = 'http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery18303457320724815821_1612713131283&fundCode=006620&pageIndex=2&pageSize=20&startDate=&endDate=&_=1612713159000'
    code = '000001'
    get_fund_info(code)