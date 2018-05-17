#  coding:utf-8
"""
用于每天运行一次来统计 allLog目录下的log日志，汇总昨天的上传ftp的文件数量以及文件行数
如果需要备份，对备份目录进行删除  备份目录按天为单位存储
删除指定日期前的log日志
"""

import configparser
import time
import logging
import datetime
import re
from collections import defaultdict
import os
import shutil

def statisticsUpload():
    needStatisticsLog = 'upload_' + yesterday +'.log'
    fileLog = open('allLog/' + needStatisticsLog, 'r')
    allMessage = fileLog.readlines()
    fileNumberDict =defaultdict(list)
    fileLenDict = defaultdict(list)
    for message in allMessage:
        if filesSUM.match(message):
            fileNumberDict[filesSUM.match(message)[1].strip()].append(int(filesSUM.match(message)[2]))
        if fileLenSum.match(message):
            fileLenDict[fileLenSum.match(message)[1].strip()].append(int(fileLenSum.match(message)[2]))

    logging.info('the number of upload files ----------------->'+str(yesterday))
    for key,valus in fileNumberDict.items():
        logging.info('the file is {} , the all is {}'.format(key,str(sum(valus))))

    logging.info('the rows of upload files ----------------->'+str(yesterday))
    for key,valus in fileLenDict.items():
        logging.info('the file is {} , the allRows is {}'.format(key,str(sum(valus))))

def delLogFile(filePath,logSaveAbsoluteSec):
    '''删除在保存时间之后的文件
    '''
    if now - os.path.getctime(filePath)<=logSaveAbsoluteSec:
        os.remove(filePath)

def main():
    #statisticsUpload()

    if logSaveDays.strip() != '':
        logSaveAbsoluteSec = int(logSaveDays)*60*60*24
        for logName in os.listdir('allLog'):
            try:
                delLogFile(os.path.join('allLog',logName),logSaveAbsoluteSec)
            except:
                logging.exception('logName del error')

    if backUpSaveDays.strip() != '':
        for fileName in os.listdir(backUpPath):
            try:
                if not os.path.isfile(os.path.join(backUpPath,fileName)):
                    backUpSaveAbsoluteSec = int(backUpSaveDays)*60*60*24
                    fileTime = time.mktime(time.strptime(fileName,'%Y%m%d'))
                    if (int(yesterday) - int(fileTime)) <= int(backUpSaveAbsoluteSec):
                        shutil.rmtree(os.path.join(backUpPath,fileName),True)
            except:
                logging.exception('fileBackUp del error')


if __name__ == '__main__':
    now = time.time()
    filesSUM = re.compile('.*?the folder is:(.*?)the number of .*?(\d+)')
    fileLenSum = re.compile('.*?statistics rows:(.*?)the lines.*?(\d+)')
    yesterday = datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(days=-1),'%Y%m%d')
    configFilePath = 'config.ini'
    cf = configparser.ConfigParser()
    cf.read(configFilePath)
    logSaveDays = cf.get("statistics","logSaveDays")  #日志的保存天数

    backUpSaveDays = cf.get("statistics","backUpSaveDays")  #备份目录的保存天数
    backUpPath = cf.get("fileMessage","backUpPath")  #备份目录的保存天数
    logging.basicConfig(filename='allLog/' + 'statistics_' + yesterday + ".log", filemode='a', level=logging.NOTSET,
                            format="%(asctime)s - %(levelname)s: %(message)s")  # 日志的输出格式
    main()


