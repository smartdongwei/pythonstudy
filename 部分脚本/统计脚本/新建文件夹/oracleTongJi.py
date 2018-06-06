
#  coding:utf-8
"""
在写脚本过程中常用的方法
"""
import os
import traceback
import shutil
import platform
import datetime
import sys
import cx_Oracle
import time
import logging

try:
    import ConfigParser    #python2 里面这个库的名字为 ConfigParser
except:
    import configparser   #python3 里面这个库的名字为 configParser

COUNT_SIZE_ROWS_DICT = {}    #该数据用于存储统计信息
WORK_SYSTEM = platform.system()   #获取脚本所在系统的名称
if WORK_SYSTEM == 'windows':
    separator = '\\'     #分隔符
else:
    separator = '/'

def getIniMessage(cf,section):
    oneMessage = []
    oneMessage.append(section)                            #获取数据库表名
    oneMessage.append(cf.get(section,'oracleUserName'))    #获取数据库登录账号
    oneMessage.append(cf.get(section,'oracleUserPwd'))    #获取数据库登录密码
    oneMessage.append(cf.get(section,'oracleIp'))    #获取数据库ip
    oneMessage.append(cf.get(section,'oraclePort'))    #获取数据库端口名称
    return oneMessage

def full_Oracle_Tongji(fullOracleIniList):
    """统计全量表的数据量
    fullOracleIniList :全量表的相关配置信息
    """
    try:
        db = cx_Oracle.connect(fullOracleIniList[1],fullOracleIniList[2], fullOracleIniList[3] + '/' + fullOracleIniList[4])  #创建数据库连接
        conn = db.cursor()   #创建游标
        sql = 'select count(*) as total from ' +fullOracleIniList[0]    #查询语句
        conn.execute(sql)     #执行sql命令
        conn_all = conn.fetchall()
        for row in conn_all:
            logging.info(fullOracleIniList[0] + '  '+ str(today) + '  '+ row)
        conn.close()        #关闭游标
        db.close()         #关闭数据库连接

    except:
        traceback.print_exc()
        logging.exception(fullOracleIniList[0])


def  Incremental_Oracle_Tongji(IncrementalOracleIniList):
    """统计增量表的指定时间的数据量
    fullOracleIniList :全量表的相关配置信息
    """
    try:
        db = cx_Oracle.connect(IncrementalOracleIniList[1],IncrementalOracleIniList[2], IncrementalOracleIniList[3] + '/' + IncrementalOracleIniList[4])  #创建数据库连接
        conn = db.cursor()   #创建游标
        timeOpen = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=IncrementalOracleIniList[6]), '%Y%m%d')   #需要查询开始时间
        timeEnd = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=IncrementalOracleIniList[7]), '%Y%m%d')    #需要查询结束时间
        sql1 = 'select %s as timeTj,count(*) as total from %s' %(IncrementalOracleIniList[5],IncrementalOracleIniList[0] )   #查询语句
        sql2 = ' where to_char(%s,\'yyyymmdd\') >=\'%s\' and to_char(%s,\'yyyymmdd\') <=\'%s\' '%(IncrementalOracleIniList[5] ,timeOpen , IncrementalOracleIniList[5],timeEnd)
        sql3 = ' group by %s order by %s '%(IncrementalOracleIniList[5],IncrementalOracleIniList[5])
        sql = sql1 +sql2 + sql3
        conn.execute(sql)     #执行sql命令
        conn_all = conn.fetchall()
        for row in conn_all:
            logging.info(IncrementalOracleIniList[0] + ' ' + str(row.timeTj) + ' ' + str(row.total) )
        conn.close()        #关闭游标
        db.close()         #关闭数据库连接

    except:
        traceback.print_exc()
        logging.exception(IncrementalOracleIniList[0])

def main():

    try:
        cf = ConfigParser.ConfigParser()             #调用函数
    except:
        cf = configparser.ConfigParser()

    cf.read(fileIniReal)              #读取配置文件
    sections = cf.sections()           #读取配置文件中所有的section  即数据库表名
    IncrementalOracleInis = []        #获取所有增量表的配置数据
    fullOracleInis = []               # 获取所有全量表的配置数据
    for section in sections:
        if cf.get(section,'isIncremental') == 'true':
            oneMessage = getIniMessage(cf,section)          # 读取配置信息
            oneMessage.append(cf.get(section,'incrementalField'))    #如果为增量表，则获取增量字段，如果为全量表，则获取信息为空
            #获取需要统计的时间段区间
            oneMessage.append(int(cf.get(section,'inquireDaysOpen')))     #统计按增量字段计算时间段区间开始的数据
            oneMessage.append(int(cf.get(section,'inquireDaysEnd')))    #统计按增量字段计算时间段区间结束的数据
            IncrementalOracleInis.append(oneMessage)
        else:
            oneMessage = getIniMessage(cf,section)         # 读取配置信息
            fullOracleInis.append(oneMessage)             #把配置信息写入到全量表对应的列表中

    for fullOracleIni in fullOracleInis:     #查询全量表数据
        full_Oracle_Tongji(fullOracleIni)
    for IncrementalOracleIni in IncrementalOracleInis:    #查询增量表数据
        Incremental_Oracle_Tongji(IncrementalOracleIni)

    '''
    #如果你想使用线程池，可以使用该方法  以增量表的查询为例
    import threadpool as tp
    from threadpool import makeRequests
    cPool1 = tp.ThreadPool(5)     # 5  表示为线程数，可以自己定义
    # 启动线程     增量表的查询
    cRequests1 = makeRequests(Incremental_Oracle_Tongji, IncrementalOracleInis)
    [cPool1.putRequest(req) for req in cRequests1]
    cPool1.wait()

    cPool2 = tp.ThreadPool(5)     # 5  表示为线程数，可以自己定义
    # 启动线程      全量表的查询
    cRequests2 = makeRequests(full_Oracle_Tongji, fullOracleInis)
    [cPool2.putRequest(req) for req in cRequests2]
    cPool2.wait()
   '''


if __name__ == '__main__':
    iniName = 'oracleConfig.ini'    #配置文件的存储位置
    today = time.strftime("%Y%m%d",time.localtime())       #脚本运行时间
    fileIniReal = os.path.split(os.path.realpath(sys.argv[0]))[0] + separator + iniName     # 配置文件的路径
    logPath = os.path.split(os.path.realpath(sys.argv[0]))[0] + separator + 'logMessage'
    if not os.path.exists(logPath):  #判断存储查询结果的文件夹是否存在，不存在就创建这个文件夹
        os.mkdir(logPath)

    logging.basicConfig(filename=logPath + separator + today + ".log", filemode='a', level=logging.NOTSET,
                        format="%(asctime)s - %(levelname)s: %(message)s")  # 日志的输出格
    main()

