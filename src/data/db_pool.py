# -*- coding: utf-8 -*-
# @Time : 2021/2/4 22:00
# @Author : Huang Zengrui
# @Email : huangzengrui@yahoo.com
# @Desc:基于Sqlalchemy的数据交互层，增加数据库中断重连机制


import os
import sys
from sqlalchemy.exc import DisconnectionError, DatabaseError, InterfaceError, OperationalError
from pandas.io.sql import DatabaseError as PandasDatabaseError

sys.path.append(os.path.join(os.path.dirname(__file__), '../../../../'))
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
root_root_Path = os.path.split(rootPath)[0]
sys.path.append(root_root_Path)
import datetime
from sqlalchemy import create_engine
from src.utils.log_tools import get_logger
from src.utils.wrappers import time_log_info
from retrying import retry
import pandas as pd
from src.config.db_config import DBConfig

# todo retry装饰器的添加，数据库重连 done
# todo 数据库连接池失效应对方案确定：1.自带悲观机制 2.自带乐观机制 3.自己实现（可以通过retry DisconnectionError, 需要指定Exception） done
# todo 数据库配置多数据源、PROD与DEV通过传入的系统参数实现自动区分 done
# todo 完善数据库事务的逻辑，如何实现多条SQL一个事务, 单条非查询SQL执行需要放在事务中执行吗？ done
# todo 批量SQL执行 done
# todo 大批量写入数据自动分批 done
# todo 读取和插入Dataframe实现 done
logger = get_logger(file_name="DB_Connect", logger_name="")
retry_kwargs_db = {
    'stop_max_attempt_number': 3,
    'retry_on_exception':
        lambda e: (isinstance(e, (DatabaseError, InterfaceError, OperationalError, PandasDatabaseError)))
}


class DBPool(object):

    def __init__(self, db_config):
        """根据数据库配置返回相应的引擎"""
        self.db_config = db_config
        self.db_type = self._get_db_type()
        self.db_engine = None
        self._init_db_engine()

    def _get_db_engine(self):
        """单例"""
        if self.db_engine is None:
            self._init_db_engine()
        return self.db_engine

    @retry(**retry_kwargs_db)
    def _init_db_engine(self):
        try:
            self.db_engine = create_engine(
                self.db_config,
                pool_size=1,  # 连接池大小,默认是1
                # max_overflow=1,  # 超过连接池大小外最多创建的连接
                # pool_timeout=1,  # 池中没有线程最多等待的时间，否则报错
                # pool_recycle=0,  # 多久之后对线程池中的线程进行一次连接的回收（重置）
                # pool_pre_ping=True  # 使用悲观机制处理连接池失效，参考：
                # https://sanyuesha.com/2019/01/02/sqlalchemy-pool-mechanism/
                # https://docs.sqlalchemy.org/en/13/core/pooling.html?highlight=pool#module-sqlalchemy.pool
            )
        except (DisconnectionError, InterfaceError, OperationalError, DatabaseError) as e:
            logger.exception("init DB engine failed by {},try to reconnect to DB...".format(e))
            raise e
        except Exception as e:
            logger.exception("init DB engine failed by {}...".format(e))

    def reconnect(self):
        self._init_db_engine()

    def generate_id(self):
        """
        oracle 生成唯一id
        :return : str
        """
        assert self.db_type == "oracle", "暂只支持oracle生成唯一id"
        sql = 'select func_generate_id from dual'
        results = self.query_by_sql(sql)
        return results[0][0]

    @time_log_info
    @retry(**retry_kwargs_db)
    def query_by_sql(self, sql, df_columns=None, to_df=False):
        """
        :param  sql:    str
        :param  df_columns:    list of str
        :param  to_df:    boolean
        :return result: list of tuple
        """
        try:
            if df_columns is None:
                assert to_df is False, "Can't return dataframe without columns!"
            with self.db_engine.connect() as conn:
                result = conn.execute(sql).fetchall()
            if to_df:
                columns = df_columns
                return pd.DataFrame(result, columns=columns)
            else:
                return result
        except (DisconnectionError, InterfaceError, OperationalError, DatabaseError) as e:
            logger.exception("Disconnect from DB engine by {}, reconnect to DB...".format(e))
            self.reconnect()
            raise e
        except Exception as e:
            logger.exception("Querying sql: {} had error:{}".format(sql, e))

    @time_log_info
    @retry(**retry_kwargs_db)
    def query_by_sql_to_df(self, sql):
        """
        通过 pandas 的 read_sql 接口实现返回 dataframe，速度较慢，但无需指定 columns
        :param sql:
        :return:
        """
        try:
            with self.db_engine.connect() as conn:
                return pd.read_sql(sql, conn)
        except (DisconnectionError, InterfaceError, OperationalError, PandasDatabaseError, DatabaseError) as e:
            logger.exception("Disconnect from DB engine by {}, reconnect to DB...".format(e))
            self.reconnect()
            raise e
        except Exception as e:
            logger.exception("Querying sql: {} had error:{}".format(sql, e))

    def query_by_condition(self, table_name, query_field, condition_field, to_df=False):
        """
        query by table name and condition
        :param table_name:      str
        :param query_field:     list of str
        :param condition_field: dict
        :param to_df:   boollean
        :return is_success: boolean
        params example:
        >>>  ("AAS_PORTFOLIO_INFO", ["userid", "portname", "initcash"], {"userid": "016901"}))
        """
        if query_field:
            query_field_str = ','.join([str(i) for i in query_field])
            sql = "select " + query_field_str + " from " + table_name + " where 1=1 "
        else:
            sql = "select * from " + table_name + " where 1=1 "
        if condition_field:
            sql = self._get_sql(sql, condition_field)
        sql = sql.replace('None', 'null')
        return self.query_by_sql(sql, query_field, to_df)

    @time_log_info
    @retry(**retry_kwargs_db)
    def insert_by_sql(self, sql):
        """
        :param sql:         str
        :return is_success: boolean
        """
        is_success = True
        try:
            with self.db_engine.connect() as conn:
                with conn.begin() as trans:
                    conn.execute(sql)
                    trans.commit()
        except (DisconnectionError, InterfaceError, OperationalError, DatabaseError) as e:
            logger.exception("Disconnect from DB engine by {}, reconnect to DB...".format(e))
            self.reconnect()
            trans.rollback()
            raise e
        except Exception as e:
            logger.exception("Inserting sql: {} had error:{}".format(sql, e))
            is_success = False
            trans.rollback()
        return is_success

    def insert_by_condition(self, table_name, insert_columns, insert_data):
        """insert by table name and condition, execute by batch automatically
        api:https://docs.sqlalchemy.org/en/13/core/connections.html?highlight=execute#sqlalchemy.engine.Connection.execute
        :param table_name     str
        :param insert_columns list of str
        :param insert_data    list of tuple
        """
        assert table_name is not None, "必须指定插入数据表名称"
        assert len(insert_columns) >= 1, "插入数据指定字段不可为空"
        assert len(insert_data) >= 1, "插入数据不可为空"

        placeholder = self._get_placeholder(insert_columns)
        insert_field_str = ','.join([str(i) for i in insert_columns])
        sql_base = "insert into " + table_name + " (" + insert_field_str + ")" + " values " + placeholder
        return self._insert_by_conn_execute(sql_base, insert_data)

    def insert_by_df(self, table_name, df):
        """
        insert data by dataframe
        :param table_name: str
        :param df: dataframe
        :return is_success: boolean
        """
        return self.insert_by_condition(table_name, df.columns, df.values.tolist())

    def _get_placeholder(self, insert_columns):
        """
        获取数据库占位符并且拼接成相应的字符串，通常  ORACLE占位符为 ：加字段名
                                             mysql占位符为 %s
                                             sqlite占位符为 ？
        :param insert_columns: list of str 字段名
        :return:
        """
        if self.db_type == "oracle":
            placeholder = "(" + ','.join(": " + column for column in insert_columns) + ")"
        elif self.db_type == "mysql":
            placeholder = "(" + ','.join("%s" for _ in range(len(insert_columns))) + ")"
        elif self.db_type == "sqlite":
            placeholder = "(" + ','.join("?" for _ in range(len(insert_columns))) + ")"
        else:
            raise Exception("No DB config for placeholder!")
        return placeholder

    @time_log_info
    @retry(**retry_kwargs_db)
    def _insert_by_conn_execute(self, sql_base, insert_data, each_batch_size=1000):
        """
        自动分批执行插入操作，默认每批插入1000条数据
        :param sql_base:
        :param insert_data:
        :return:
        """
        is_success = True
        try:
            with self.db_engine.connect() as conn:
                with conn.begin() as trans:
                    total_data_size = len(insert_data)
                    for idx in range(0, total_data_size, each_batch_size):
                        try:
                            conn.execute(sql_base, insert_data[idx: idx + each_batch_size])
                        except Exception as e:
                            # 此处是为了只在出错时日志记录插入的单批次数据，重新抛出异常的目的是为了终止插入操作并回滚
                            logger.exception("Inserting batch data: {} {} had error:{}"
                                             .format(sql_base, insert_data[idx: idx + each_batch_size], e))
                            raise e
                    trans.commit()
        except (DisconnectionError, InterfaceError, OperationalError, DatabaseError) as e:
            logger.exception("Disconnect from DB engine by {},try to reconnect to DB...".format(e))
            self.reconnect()
            trans.rollback()
            raise e
        except Exception as e:
            logger.exception("Inserting sql: {} had error:{}".format(sql_base, e))
            is_success = False
            trans.rollback()
        return is_success

    @time_log_info
    @retry(**retry_kwargs_db)
    def update_by_sql(self, sql):
        """
        :param  sql: str
        :return is_success: boolean
        """
        is_success = True
        try:
            with self.db_engine.connect() as conn:
                with conn.begin() as trans:
                    conn.execute(sql)
                    trans.commit()
        except (DisconnectionError, InterfaceError, OperationalError, DatabaseError) as e:
            logger.exception("Disconnect from DB engine by {}, reconnect to DB...".format(e))
            self.reconnect()
            trans.rollback()
            raise e
        except Exception as e:
            logger.exception("Updating sql: {} had error:{}".format(sql, e))
            is_success = False
            trans.rollback()
        return is_success

    def update_by_condition(self, table_name, update_field, condition_field):
        """
        :param table_name: str
        :param update_field: list of str
        :param condition_field: dict
        :return is_success: boolean
        """
        sql = "UPDATE {} SET ".format(table_name)
        sql = self._get_sql(sql, update_field)
        sql = sql.replace('and', ',')
        index_ = sql.index(',')
        sql = sql[:index_ - 1] + sql[index_ + 1:]
        sql += ' WHERE 1=1 '
        sql = self._get_sql(sql, condition_field)
        sql = sql.replace('None', 'null')
        return self.update_by_sql(sql)

    @time_log_info
    @retry(**retry_kwargs_db)
    def del_by_sql(self, sql):
        """
        执行删除sql
        :param sql: str
        :return is_success: boolean
        """
        is_success = True
        try:
            with self.db_engine.connect() as conn:
                with conn.begin() as trans:
                    conn.execute(sql)
                    trans.commit()
        except (DisconnectionError, InterfaceError, OperationalError, DatabaseError) as e:
            logger.exception("Disconnect from DB engine by {}, reconnect to DB...".format(e))
            self.reconnect()
            trans.rollback()
            raise e
        except Exception as e:
            logger.exception("Deleting sql: {} had error:{}".format(sql, e))
            is_success = False
            trans.rollback()
        return is_success

    def del_by_condition(self, table_name, del_condition):
        """
        删除语句，调用此接口务必小心，del_condition为空则全表删除
        :param table_name: str
        :param del_condition: dict
        :return is_success: boolean
        """
        sql = "delete from {} where 1=1".format(table_name)
        if del_condition:
            sql = self._get_sql(sql, del_condition)
        else:
            logger.warn("delete all data from {} !!!".format(table_name))
        return self.del_by_sql(sql)

    @staticmethod
    def _get_sql(sql, _dict):
        """
        拼接 condition_field 为 sql 格式
        :param sql:   str
        :param _dict: dict
        :return sql:  str
        """
        for k, v in _dict.items():
            if type(v) == str:
                sql += ' and ' + k + ' = ' + '\'' + v + '\''
            elif type(v) == datetime.date:
                sql += ' and ' + k + ' = ' + 'DATE' + '\'' + str(v) + '\''
            elif type(v) == datetime.datetime:
                sql += ' and ' + k + ' = ' + 'TIMESTAMP' + '\'' + str(v) + '\''
            else:
                sql += ' and ' + k + ' = ' + str(v)
        return sql

    def _get_db_type(self):
        """
        从配置文件获取数据库类型，影响占位符生成格式
        :return:
        """
        if "oracle" in self.db_config:
            return "oracle"
        elif "mysql" in self.db_config:
            return "mysql"
        elif "sqlite" in self.db_config:
            return "sqlite"
        else:
            raise Exception("No DB config for DB type!")

    @time_log_info
    @retry(**retry_kwargs_db)
    def execute_batch_sqls(self, sql_list):
        """
        对于增删改的批量操作进行事务执行，不包括查表操作
        :param  sql_list:    list of str
        :return result:list of tuple
        """
        is_success = True
        try:
            with self.db_engine.connect() as conn:
                with conn.begin() as trans:
                    for sql in sql_list:
                        conn.execute(sql)
                    trans.commit()
        except (DisconnectionError, InterfaceError, OperationalError, DatabaseError) as e:
            logger.exception("Disconnect from DB engine by {}, reconnect to DB...".format(e))
            self.reconnect()
            trans.rollback()
            raise e
        except Exception as e:
            is_success = False
            logger.exception("execute batch sqls failed, had error:{}".format(sql, e))
            trans.rollback()
        return is_success


# 测试
if __name__ == '__main__':

#
# # 写表
# # test for insert_by_sql
    print(DBPool(DBConfig.mysql_url).
          insert_by_sql("insert into fund_company_info(company_id, company_name) values(20000, 'A')"))
#
# # test for insert_by_condition
# print(DBPool(DBConfig.MYSQL_01).
#       insert_by_condition("MYSQL_TEST_TABLE",
#                           ["USERID", "USERNAME", "GENDER", "AGE"],
#                           [
#                               ["000349", "a", 0, 19],
#                               ["000330", "a", 0, 19],
#                               ["000331", "a", 0, 19],
#                           ]
#                           ))
#
# test for insert_by_df
# df = pd.DataFrame({"USERID": ["000500", "000501", "000502"], "USERNAME": ["a", "b", "c"], "GENDER": [1, 0, 0],
#                    "AGE": [23, 23, 24]})
# # print(df)
# print(DBPool(DBConfig.ORC_01).insert_by_df("oracle_test_table", df))
#
# # 查表
# # test for query_by_sql
#
# print(DBPool(DBConfig.ORC_01).query_by_sql(
#     "select USERID, USERNAME, GENDER from oracle_test_table where USERNAME='a'",
#     df_columns=["USERID", "USERNAME", "GENDER"],
#     to_df=True))
#
# # test for query_by_condition
# print(DBPool(DBConfig.ORC_01).
#       query_by_condition("oracle_test_table",
#                          ["USERID", "USERNAME", "GENDER"],
#                          {"age": 19}, to_df=True
#                          ))
# # 删表
# # test for del_by_sql
# print(DBPool(DBConfig.ORC_01).del_by_sql("delete from oracle_test_table where userid='000019'"))
#
# # test for del_by_condition
# print(DBPool(DBConfig.ORC_01).del_by_condition("oracle_test_table", {"userid": "000020"}))
#
# # 更新表
# # test for update_by_sql
# print(DBPool(DBConfig.ORC_01).update_by_sql("update oracle_test_table set age=22, gender=0 where age=19"))
#
# # test for update_by_condition
# today = datetime.date.today()
# print(DBPool(DBConfig.ORC_01).update_by_condition("oracle_test_table",
#                                                   {"birthday": today},
#                                                   {"age": 22}
#                                                   ))
