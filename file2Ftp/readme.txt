需要自己安装threadpool 模块

file2ftp.py     把本地文件上传到ftp上
DailyStatisticsLog.py   统计模块以及删除备份 日志模块
config.ini      配置信息


在配置文件中
[ftpMessage]
host = 192.168.63.101
port = 21
username = test
passwd = test    
ftpRootPath =  /CD/DEMO/     #ftp存储的根目录

[fileMessage]
fileRoot = E:\\ceshi            #本地文件存储的根目录
needUploadFileHouZhui = txt     #需要上传的文件后缀名   如果为空，则上传所有的文件
LinShiHouZhui = .img            #临时后缀名，
isBackUp = 1                    #  1  需要备份    0 不需要备份
backUpPath = E:\\temp           #需要备份的路径
threadPoolNumber = 3            # 线程池的线程数  一般不大于为二级目录的目录数


[statistics]
logSaveDays= 10                  #主要用于确定日志需要保存多少天
backUpSaveDays = 10              #主要用于确定备份目录需要保存多少天    如果不需要删除，不需要写


在日志目录下生成两种日志
statistics_20180516.log              #生成每天上传量的日志
upload_20180516.log                  #每次脚本运行时生成的日志


在脚本运行时会创建一个  临时文件 needRemove.md  如果脚本运行结束后该文件没有被删除，说明出了问题，手动删除该文件
在脚本运行过程中不用管这个文件
