# md_recorder
record market data for futures listed in mainland China through CTP

##dependency: 
  Python 3.10
  Windows Redis
  
##使用须知: 在Windows 10/11 及以上版本，安装Python3.10(pyd文件是在python3.10版本下编译的，若后续想升级Python版本至3.11及以后版本，需自行编译对应的pyd文件)，
安装Windows Redis并加入系统服务。将md_recorder_redis.bat加入windows 任务计划，每日开盘前启动即可，收盘后程序自动将当日tick数据存储为csv文件并压缩保存。
setting.json文件存放穿透式认证信息和账户信息，以便通过交易接口获取当日交易合约列表。
