import requests
import pandas as pd
import re
import sys
import math
import datetime
import time
from src.utils.log_tools import get_logger
from .spider_exception import *
from src.config.global_config import *
from src.data.db_pool import DBPool
from src.config.db_config import DBConfig

logger = get_logger(file_name="spider", logger_name="main")
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


def get_resonse(url):
    """
    :param url: 网页URL
    :return: 爬取的文本信息
    """
    try:
        r = requests.get(url)
        r.raise_for_status()
        r.encoding = 'utf-8'
        return r.text
    except:
        print('Failed to get response to url!')
        return ''


def get_company_list(url=None):
    """
    :param url: 基金公司信息的URL
    :return: 将结果存储在当前目录Data/company_list.csv中
    """
    now_timestamp = datetime.datetime.timestamp(datetime.datetime.now())
    url = 'http://fund.eastmoney.com/js/jjjz_gs.js?dt={}'.format(now_timestamp)
    logger.info("开始获取基金公司ID信息...")
    response = get_resonse(url)
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
    pool.insert_by_df("fund_company_info", df)
    df.to_csv('local_data/company_list.csv', index=False)
    logger.info("基金公司ID写入DB与CSV完成<<<<<====")
    logger.info("基金公司ID信息爬取完成<<<<<====")

def get_fund_list(url):
    """
    :param url: 基金概况信息的URL
    :return: 将基金统计信息存入当前目录Data/fund_list.csv中,返回基金代码号列表
    """
    data = {}
    response = get_resonse(url)
    code_list = []
    abbreviation_list = []
    name_list = []
    type_list = []
    name_en_list = []
    tmp = re.findall(r"(\".*?\")", response)
    for i in range(0, len(tmp)):
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
    data['code'] = code_list
    data['abbreviation'] = abbreviation_list
    data['name'] = name_list
    data['type'] = type_list
    data['name_en'] = name_en_list
    df = pd.DataFrame(data)
    df.to_csv('local_data/fund_list.csv')
    return code_list


def get_fund_info(code):
    failed_list = []
    data_list = {}
    url = 'http://fund.eastmoney.com/pingzhongdata/' + code + '.js'
    response = get_resonse(url)
    # 爬取失败等待再次爬取
    if response == '':
        return ''
    else:
        strs = re.findall(r'var(.*?);', response)
        for i in range(0, len(strs)):
            tmp = strs[i].split('=')
            var_name = tmp[0].strip()
            data_list[var_name] = [tmp[1]]
        return data_list


def get_pingzhong_data():
    data = pd.read_csv('local_data\instruments_ansi.csv', encoding='ANSI')
    code_list = data['code']
    data = {'fS_name': [],
            'fS_code': [],
            'fund_sourceRate': []
        , 'fund_Rate': []
        , 'fund_minsg': []
        , 'stockCodes': []
        , 'zqCodes': []
        , 'syl_1n': []
        , 'syl_6y': []
        , 'syl_3y': []
        , 'syl_1y': []
        , 'Data_holderStructure': []
        , 'Data_assetAllocation': []
        , 'Data_currentFundManager': []
        , 'Data_buySedemption': []}
    failed_list = []
    for i in range(0, len(code_list)):
        code = '%06d' % code_list[i]
        progress = i / len(code_list) * 100
        print('爬取' + code + '中，进度', '%.2f' % progress + '%')
        progress_bar(i, len(code_list))
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
    df.to_csv('local_data/results.csv')
    df_fail = pd.DataFrame(failed_list)
    df_fail.to_csv('local_data/fail.csv')


def download_f10_ts_data():
    data = pd.read_csv('local_data\instruments_ansi.csv')
    code_list = data['code']
    for i in range(0, len(code_list)):
        progress_bar(i, len(code_list))
        name = '%06d' % code_list[i]
        url = 'http://fund.eastmoney.com/f10/tsdata_' + name + '.html'
        file_name = 'Data/f10_ts/' + name + '.json'
        response = get_resonse(url)
        with open(file_name, 'w', encoding='utf-8') as f:
            print(response, file=f)


def download_manager_info():
    data = pd.read_csv('Data/instruments_ansi.csv', encoding='ANSI')
    code_list = data['code']
    for i in range(0, len(code_list)):
        progress_bar(i, len(code_list))
        name = '%06d' % code_list[i]
        url = 'http://fundf10.eastmoney.com/jjjl_' + name + '.html'
        file_name = 'Data/managerInfo/' + name + '.json'
        response = get_resonse(url)
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
        response = get_resonse(url)
        with open(file_name, 'w', encoding='utf-8') as f:
            print(response, file=f)


if __name__ == '__main__':
    # download_manager_info()
    # solve_f10_data()
    # solve_fund_info()
    download_risk_info()