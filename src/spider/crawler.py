#!usr/bin/env python
# -*- coding: utf-8 -*-
# **************************************************
# @Time : 2021/2/24 12:47
# @Author : Huang Zengrui
# @Email : huangzengrui@yahoo.com
# @Desc :
# **************************************************


import requests
import pandas as pd
import re
import os
import sys
import math
import datetime
import time
from src.utils.log_tools import get_logger
from src.spider.spider_exception import *
from src.config.global_config import *
from src.data.db_pool import DBPool
from src.config.db_config import DBConfig
from src.utils.tools import *

if not os.path.exists("./local_data"):
    os.makedirs("./local_data")
logger = get_logger(file_name="spider", logger_name="spider")
if TO_DB:
    pool = DBPool(DBConfig.mysql_url)


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


class FundSpider(object):

    def __init__(self, mode):
        """
        :param mode: "daily" or "once"
        """
        self.mode = mode
        self.time_now = datetime.datetime.now()

    def begin_crawler(self):
        """
        这个函数决定每天什么时刻运行什么函数更新什么数据
        爬虫日程
        :return:
        """
        # 暂定皆为日更, 22:00

        while True:
            if self.mode == "once":
                self.update_data()
                self.process_data()
                break
            self.time_now = datetime.datetime.now()
            if self.mode == "daily" and self.time_now.hour == 20 \
                    and self.time_now.minute == 54 and self.time_now.second == 0:
                self.update_data()
                time.sleep(2)
            if self.mode == "daily" and self.time_now.hour == 3 \
                    and self.time_now.minute == 0 and self.time_now.second == 0:
                self.process_data()
                time.sleep(2)

    def update_data(self):
        logger.info("====>>>>>开始执行爬虫 with mode < {} >...".format(self.mode))
        time_s = datetime.datetime.now()
        self.get_fund_list()
        self.get_fund_company_list()
        self.get_fund_info()
        logger.info("====>>>>>爬虫总共执行时间为：{:.2f} min".format
                    ((datetime.datetime.now() - time_s).total_seconds() / 60))

    def process_data(self):
        logger.info("====>>>>>开始处理数据 with mode < {} >...".format(self.mode))
        time_s = datetime.datetime.now()
        self.process_fund_data()
        self.process_special_data()
        self.process_manager_data()
        logger.info("====>>>>>数据处理时间总共执行时间为：{:.2f} min".format
                    ((datetime.datetime.now() - time_s).total_seconds() / 60))

    @staticmethod
    def get_fund_list():
        """
        :return: 将基金统计信息存入当前目录./local_data/fund_list.csv中,返回基金代码号列表
        """
        logger.info("*******************************************************************")
        logger.info("< 基金列表信息 >开始获取...")
        data = {}
        url = 'http://fund.eastmoney.com/js/fundcode_search.js'
        response = get_response(url)
        fund_code_list = []
        abbreviation_list = []
        name_list = []
        type_list = []
        name_en_list = []
        time_list = []
        tmp = re.findall(r"(\".*?\")", response)
        total_length = len(tmp)
        for i in range(0, total_length):
            if NEED_SLEEP:
                time.sleep(SLEEP_TIME_MIN)
            if i % 5 == 0:
                fund_code_list.append(eval(tmp[i]))
                time_list.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            elif i % 5 == 1:
                abbreviation_list.append(eval(tmp[i]))
            elif i % 5 == 2:
                name_list.append(eval(tmp[i]))
            elif i % 5 == 3:
                type_list.append(eval(tmp[i]))
            else:
                name_en_list.append(eval(tmp[i]))

        logger.info("< 基金列表信息 >共有 {} 条".format(len(fund_code_list)))
        data['fund_id'] = fund_code_list
        data['fund_name'] = name_list
        data['fund_abbr'] = abbreviation_list
        data['fund_type'] = type_list
        data['update_time'] = time_list
        # 基金类型种类 {'其他创新', '分级杠杆', '混合型', 'QDII-指数', '混合-FOF', '联接基金', '理财型', '货币型', '定开债券',
        # '债券型', '股票指数', '股票型', 'ETF-场内', '债券指数', '固定收益', 'QDII', '股票-FOF', 'QDII-ETF'}
        logger.info("< 基金列表信息 >包含所有的基金类型为 {} ".format(set(type_list)))
        # data['name_en'] = name_en_list
        df = pd.DataFrame(data)
        df.to_csv('local_data/fund_list.csv', index=False)
        if TO_DB:
            pool.insert_by_df("fund_info", df)
        logger.info("< 基金列表信息 >写入 DB 与 CSV 完成")
        logger.info("< 基金列表信息 >爬取完成 <<<<<==========")
        logger.info("*******************************************************************")
        return fund_code_list

    @staticmethod
    def get_fund_company_list():
        """
        :return: 将结果存储在当前目录 ./local_data/company_list.csv中
        """
        now_timestamp = datetime.datetime.timestamp(datetime.datetime.now())
        url = 'http://fund.eastmoney.com/js/jjjz_gs.js?dt={}'.format(now_timestamp)
        logger.info("*******************************************************************")
        logger.info("< 基金公司列表信息 >开始获取...")
        response = get_response(url)
        if response:
            pass
        else:
            logger.warning("< 基金公司列表信息 >获取失败，URL没有响应...")
            raise NetworkException("< 基金公司列表信息 >获取失败，URL没有响应...")
        code_list = []
        name_list = []
        time_list = []
        tmp = re.findall(r"(\".*?\")", response)
        total_length = len(tmp)
        for i in range(0, len(tmp)):
            if NEED_SLEEP:
                time.sleep(SLEEP_TIME_MIN)
            if i % 2 == 0:
                code_list.append(tmp[i].strip("\""))
                time_list.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            else:
                name_list.append(tmp[i].strip("\""))
        logger.info("< 基金公司列表信息 >共获取 {} 条".format(total_length))
        data = {'company_id': code_list, 'company_name': name_list, "update_time": time_list}
        df = pd.DataFrame(data)
        if TO_DB:
            pool.insert_by_df("fund_company_info", df)
        df.to_csv('local_data/company_list.csv', index=False)
        logger.info("< 基金公司列表信息 >写入 DB 与 CSV 完成")
        logger.info("< 基金公司列表信息 >爬取完成 <<<<<==========")
        logger.info("*******************************************************************")

    def get_fund_info(self):
        """
        这个函数获取基金详情页面数据
        [get_fund_info共耗时2853.12s, 失败178个]
        :return:
        """
        logger.info("*******************************************************************")
        logger.info("< 基金详情信息 >开始获取...")
        fund_code_df = pd.read_csv('local_data//fund_list.csv', dtype=str)
        code_list = fund_code_df['fund_id']
        data = {'fS_name': [],  # 基金名  "华夏成长混合"
                'fS_code': [],  # 基金ID  "000001"
                'fund_sourceRate': [],  # 原费率  "1.50"
                'fund_Rate': [],  # 现费率  "0.15"
                'fund_minsg': [],  # 最小申购金额  "100"
                'stockCodes': [],  # 最新持仓前十大股票代码
                # ["6013181","0021272","0009612","6005191","0022712","6000481","0005472","0020132","0005132","0008582"]

                'zqCodes': [],  # 基金持仓债券代码
                'stockCodesNew': [],  # 基金持仓股票代码(新市场号)
                'zqCodesNew': [],  # 基金持仓债券代码（新市场号）
                'syl_1n': [],  # 近一年收益率  "42.74"
                'syl_6y': [],  # 近6月收益率  "9.34"
                'syl_3y': [],  # 近3月收益率  "11.56"
                'syl_1y': [],  # 近1月收益率  "11.88"
                'Data_fundSharesPositions': [],  # 股票仓位测算图（推测仓位）
                # [[1610553600000,95.00],[1610640000000,95.00],[1610899200000,84.200],[1610985600000,97.0300],[1611072000000,93.6100],[1611158400000,92.00],[1611244800000,96.0600],[1611504000000,91.9700],[1611590400000,95.00],[1611676800000,96.3900],[1611763200000,95.00],[1611849600000,95.00],[1612108800000,95.00],[1612195200000,95.00],[1612281600000,95.00],[1612368000000,97.200],[1612454400000,95.00],[1612713600000,95.00],[1612800000000,95.00]]

                'Data_netWorthTrend': [],  # list of dict -> [{x:时间戳  y:单位净值  equityReturn:净值回报 unitMoney:每份派送金}]
                'Data_ACWorthTrend': [],  # list -> [[时间戳, 累计净值]]
                'Data_grandTotal': [],
                # 累计收益率走势
                # list of dict -> [{"name":"华夏成长混合", data=[[时间戳, 累计收益率]]}，
                #                  {"name":"同类平均", data=[[时间戳, 累计收益率]]}，
                #                  {"name":"沪深300", data=[[时间戳, 累计收益率]]}]

                'Data_rateInSimilarType': [],  # 同类排名 [{"x":时间戳,"y":排名,"sc":总数}]
                'Data_rateInSimilarPersent': [],  # 同类排名百分比 [[时间戳， 百分比]]
                'Data_fluctuationScale': [],
                # 规模变动  mom：较上期环比  y:净资产规模（亿元）
                # {"categories":["2019-12-31","2020-03-31","2020-06-30","2020-09-30","2020-12-31"],"series":[{"y":45.98,"mom":"0.48%"},{"y":43.92,"mom":"-4.48%"},{"y":50.55,"mom":"15.08%"},{"y":49.94,"mom":"-1.20%"},{"y":48.10,"mom":"-3.69%"}]}

                'Data_holderStructure': [],
                #  持有人结构
                #  {"series":[{"name":"机构持有比例","data":[0.3,0.4,0.45,4.88]},{"name":"个人持有比例","data":[99.7,99.6,99.55,95.12]},{"name":"内部持有比例","data":[0.01,0.0,0.0,0.0]}],"categories":["2018-12-31","2019-06-30","2019-12-31","2020-06-30"]}

                'Data_assetAllocation': [],
                # 资产配置
                # {"series":[{"name":"股票占净比","type":null,"data":[63.27,77.61,74.88,75.28],"yAxis":0},{"name":"债券占净比","type":null,"data":[26.6,20.52,21.61,20.33],"yAxis":0},{"name":"现金占净比","type":null,"data":[6.21,3.45,1.65,5.03],"yAxis":0},{"name":"净资产","type":"line","data":[43.9232812569,50.5485872045,49.9421258086,48.101648276],"yAxis":1}],"categories":["2020-03-31","2020-06-30","2020-09-30","2020-12-31"]}

                'Data_performanceEvaluation': [],
                # 业绩评价 ['选股能力', '收益率', '抗风险', '稳定性','择时能力']
                # {"avr":"51.50","categories":["选证能力","收益率","抗风险","稳定性","择时能力"],"dsc":["反映基金挑选证券而实现风险\u003cbr\u003e调整后获得超额收益的能力","根据阶段收益评分，反映基金的盈利能力","反映基金投资收益的回撤情况","反映基金投资收益的波动性","反映基金根据对市场走势的判断，\u003cbr\u003e通过调整仓位及配置而跑赢基金业\u003cbr\u003e绩基准的能力"],"data":[30.0,50.0,50.0,60.0,70.0]}

                'Data_currentFundManager': [],
                # 基金经理信息
                # [{"id":"30198442","pic":"https://pdf.dfcfw.com/pdf/H8_JPG30198442_1.jpg","name":"董阳阳","star":3,"workTime":"7年又342天","fundSize":"83.04亿(6只基金)","power":{"avr":"53.65","categories":["经验值","收益率","抗风险","稳定性","择时能力"],"dsc":["反映基金经理从业年限和管理基金的经验","根据基金经理投资的阶段收益评分，反映\u003cbr\u003e基金经理投资的盈利能力","反映基金经理投资的回撤控制能力","反映基金经理投资收益的波动","反映基金经理根据对市场的判断，通过\u003cbr\u003e调整仓位及配置而跑赢业绩的基准能力"],"data":[87.10,23.70,68.80,80.0,65.70],"jzrq":"2021-02-10"},"profit":{"categories":["任期收益","同类平均","沪深300"],"series":[{"data":[{"name":null,"color":"#7cb5ec","y":63.9657},{"name":null,"color":"#414c7b","y":170.74},{"name":null,"color":"#f7a35c","y":59.39}]}],"jzrq":"2021-02-10"}}]

                'Data_buySedemption': [],
                # 申购赎回
                # {"series":[{"name":"期间申购","data":[4.35,1.43,2.53,2.00]},{"name":"期间赎回","data":[5.55,3.00,5.36,2.62]},{"name":"总份额","data":[40.42,38.85,36.02,35.41]}],"categories":["2020-03-31","2020-06-30","2020-09-30","2020-12-31"]}

                'swithSameType': []
                # 同类基金
                # [['008437_九泰行业优选混合A_25.71','008438_九泰行业优选混合C_25.69','008293_农银汇理创新医疗混合_23.78','000594_大摩进取优选股票_23.02','004040_金鹰医疗健康产业A_22.60'],['005968_创金合信工业周期股票_56.09','005969_创金合信工业周期股票_55.82','001951_金鹰改革红利混合_49.59','002959_汇添富盈泰混合_47.95','009644_东方阿尔法优势产业混_46.51'],['005968_创金合信工业周期股票_90.71','005969_创金合信工业周期股票_90.06','161725_招商中证白酒指数(L_81.80','001951_金鹰改革红利混合_76.99','160632_鹏华酒_76.65'],['005968_创金合信工业周期股票_183.69','005969_创金合信工业周期股票_181.75','570001_诺德价值优势混合_170.12','161725_招商中证白酒指数(L_167.77','160632_鹏华酒_167.57'],['001679_前海开源中国稀缺资产_462.09','002079_前海开源中国稀缺资产_461.04','005176_富国精准医疗混合_324.56','004851_广发医疗保健股票A_299.19','003095_中欧医疗健康混合A_294.93']]
                }

        failed_list = []
        time_s = time.time()
        for idx, fund_code in enumerate(code_list):
            if DEBUG and idx == 10:
                break
            if NEED_SLEEP:
                time.sleep(SLEEP_TIME_MIN)
            progress = idx / len(code_list) * 100
            print('\r 爬取' + fund_code + '中，进度', '%.2f' % progress + '% ', end='')
            # progress_bar(idx, len(code_list))
            fund_info = self._get_fund_info(fund_code)
            if fund_info == '':
                failed_list.append(fund_code)
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
        df.to_csv('local_data/fund_info_raw.csv', index=False)
        df_fail = pd.DataFrame(failed_list, columns=["failed_fund_code"])
        df_fail.to_csv('local_data/failed_fund_code.csv', index=False)
        logger.info("< 基金详情信息 >爬取成功 {} 条".format(len(data["fS_code"])))
        logger.info("< 基金详情信息 >爬取失败 {} 条".format(len(failed_list)))
        logger.info("< 基金详情信息 >写入 CSV 成功")
        logger.info("< 基金详情信息 >失败列表写入 CSV 成功")
        logger.info("< 基金详情信息 >爬取完成,共耗时{:.2f} min <<<<<==========".format((time.time() - time_s) / 60))
        logger.info("*******************************************************************")

    @staticmethod
    def _get_fund_info(code):
        """
        解析基金详情页面数据
        :param code:
        :return:
        """
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
                # print("\n\n==============================")
                # print(var_name)
                # print([tmp[1]])
            return data_list

    @staticmethod
    def get_special_data():
        """
        基金风险、投资风格、业绩评价数据，保存成本地json文件
        :return:
        """
        logger.info("*******************************************************************")
        logger.info("< 基金特色数据 >开始获取...")
        time_s = time.time()
        if not os.path.exists("./local_data/special_data"):
            os.makedirs("./local_data/special_data")
        data = pd.read_csv('local_data//fund_list.csv')
        code_list = data['fund_id']
        failed_list = []
        success_count = 0
        for i in range(0, len(code_list)):
            if NEED_SLEEP:
                time.sleep(SLEEP_TIME_MIN)
            # progress_bar(i, len(code_list))
            name = '%06d' % code_list[i]
            progress = i / len(code_list) * 100
            url = 'http://fund.eastmoney.com/f10/tsdata_' + name + '.html'
            print('\r 爬取' + name + '中，进度', '%.2f' % progress + '% {}'.format(url), end='')
            file_name = 'local_data/special_data/' + name + '.json'
            response = get_response(url)
            if not response:
                failed_list.append(i)
            else:
                success_count += 1
            with open(file_name, 'w', encoding='utf-8') as f:
                print(response, file=f)
        failed_df = pd.DataFrame(failed_list, columns=["Failed_Code"])
        failed_df.to_csv("local_data/special_data/failed_code_list", index=False)
        logger.info("< 基金特色数据 >爬取成功 {} 条".format(success_count))
        logger.info("< 基金特色数据 >爬取失败 {} 条".format(len(failed_list)))
        logger.info("< 基金特色数据 >写入 CSV 成功")
        logger.info("< 基金特色数据 >失败列表写入 CSV 成功")
        logger.info("< 基金特色数据 >爬取完成,共耗时{:.2f} min <<<<<==========".format((time.time() - time_s) / 60))
        logger.info("*******************************************************************")

    @staticmethod
    def get_manager_info():
        """
        获取基金经理数据，保存成本地json文件
        :return:
        """
        logger.info("*******************************************************************")
        logger.info("< 基金经理数据 >开始获取...")
        time_s = time.time()
        if not os.path.exists("./local_data/manager_data"):
            os.makedirs("./local_data/manager_data")
        data = pd.read_csv('local_data//fund_list.csv')
        code_list = data['fund_id']
        failed_list = []
        success_count = 0
        for i in range(0, len(code_list)):
            # progress_bar(i, len(code_list))
            name = '%06d' % code_list[i]
            url = 'http://fundf10.eastmoney.com/jjjl_' + name + '.html'
            file_name = 'local_data/manager_data/' + name + '.json'
            response = get_response(url)
            if not response:
                failed_list.append(i)
            else:
                success_count += 1
            with open(file_name, 'w', encoding='utf-8') as f:
                print(response, file=f)
        failed_df = pd.DataFrame(failed_list, columns=["Failed_Code"])
        failed_df.to_csv("local_data/manager_data/failed_code_list", index=False)
        logger.info("< 基金经理数据 >爬取成功 {} 条".format(success_count))
        logger.info("< 基金经理数据 >爬取失败 {} 条".format(len(failed_list)))
        logger.info("< 基金经理数据 >写入 CSV 成功")
        logger.info("< 基金经理数据 >失败列表写入 CSV 成功")
        logger.info("< 基金经理数据 >爬取完成,共耗时{:.2f} min <<<<<==========".format((time.time() - time_s) / 60))
        logger.info("*******************************************************************")

    @staticmethod
    def get_risk_info():
        if not os.path.exists("./local_data/risk_data"):
            os.makedirs("./local_data/risk_data")
        data = pd.read_csv('local_data//fund_list.csv')
        code_list = data['code']
        for i in range(0, len(code_list)):
            # progress_bar(i, len(code_list))
            name = '%06d' % code_list[i]
            url = 'http://fund.eastmoney.com/' + name + '.html'
            file_name = 'local_data/risk_data/' + name + '.json'
            response = get_response(url)
            with open(file_name, 'w', encoding='utf-8') as f:
                print(response, file=f)

    def process_fund_data(self):
        pass

    def process_special_data(self):
        pass

    def process_manager_data(self):
        pass

    def get_fund_net_val(self, from_init=False):
        """
        每日更新
        这个函数获取基金每日净值数据
        :param from_init:boolean 是否从最开始更新净值，默认否，即指增量更新最新的净值
        :return:
        """
        pass


if __name__ == '__main__':
    FundSpider("once").begin_crawler()
