# @Time : 2021/2/4 22:38 
# @Author : Huang Zengrui
# @Email : huangzengrui@yahoo.com
# @Desc:

# 是否保存到数据库
TO_DB = False

# 是否使用Debug模式
DEBUG = False

# 请求之间需要睡眠
NEED_SLEEP = False

# 睡眠时间
SLEEP_TIME = 0.5
SLEEP_TIME_MIN = 0.01


retry_request = True
retry_db = True

retry_request_kwargs = {}
retry_request_db = {}