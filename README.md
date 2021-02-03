# fund_crawler


重构自项目https://github.com/XDTD/fund_crawler

基金爬虫，爬取天天基金的基金信息、基金经理信息、公司列表

## 主要URL

1. 公司列表(Github上直接打开好像会提示not found,复制到浏览器上方直接进入即可)：[http://fund.eastmoney.com/js/jjjz_gs.js]()
2. 基金列表：http://fund.eastmoney.com/js/fundcode_search.js
3. 基金信息1：http://fund.eastmoney.com/pingzhongdata/'+code+'.js‘  其中,code为6位整数，如000001的URL位=为http://fund.eastmoney.com/pingzhongdata/000001.js
4. 基金信息2:http://fund.eastmoney.com/f10/tsdata_'+code+'.html'，同上
5. 基金经理信息:http://fundf10.eastmoney.com/jjjl_'+code+'.html',同上


## 使用方法

进入main.py执行即可

注：除了solve开头的函数依赖于之前函数的下载文件，其他函数之间相互独立无先后顺序可以分别执行

数据量太大只上传部分关键数据


## todo

1.基金每日净值数据爬取
2.数据库连接层实现
3.项目结构调整
4.使用OOP完全重构
5.添加定时任务，迁移至Linux服务器运行