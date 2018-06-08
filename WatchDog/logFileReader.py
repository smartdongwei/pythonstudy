# -*- coding: utf-8 -*-
import os
import time
import datetime
import json
import common
import dbMgr
import cfgMgr
import dataSend


def sacnPath(logPath):
    curDate = datetime.datetime.now().strftime('%Y%m%d')
    fileList = []

    for parent, dirs, files in os.walk(logPath):
        for file in files:
            if file.find(curDate) < 0:  # 只扫描今天的数据
                continue
            # "310000_000101_01_全部_20110520_0.shlog"
            if len(file) < 16:  # 日志名长度太短
                continue

            if file[-6:] != ".shlog":  # 非日志文件不处理
                continue

            filename = os.path.join(parent, file)
            if os.path.isfile(filename):
                fileList.append(filename)

        return fileList

    pass


def needRead(logFile):
    # 文件读取完毕,且新文件已经生成
    cur = dbMgr.GetDBCur()
    sql = "select needread from logstat where filename='%s'" % logFile
    cur.execute(sql)
    row = cur.fetchone()
    if row:
        needread = row[0]
    else:
        needread = 1

    return needread

    pass


fileNameToHandle = {}


def GetFileHandle(fileName, offset):
    global fileNameToHandle
    if fileName in fileNameToHandle:
        return fileNameToHandle[fileName]
    try:
        fd = open(fileName, "rt")
        fd.seek(offset)
    except Exception, e:
        print("open logfile error:%s Exception:%s" % (fileName, e))
        return None

    fileNameToHandle[fileName] = fd
    return fd


def DelFileHandle(fileName):
    global fileNameToHandle
    if fileName in fileNameToHandle:
        fd = fileNameToHandle[fileName]
        if fd:
            fd.close()
        fileNameToHandle.pop(fileName)


def ReadLog(logFile):
    cur = dbMgr.GetDBCur()
    sql = "select offset from logstat where filename='%s'" % logFile
    cur.execute(sql)
    row = cur.fetchone()
    if row:
        offset = row[0]
    else:
        sql = "insert into logstat(offset,filename,needread,date )values(0,'%s',1,'%s')" % (
            logFile, datetime.datetime.now().strftime('%Y%m%d'))
        cur.execute(sql)
        offset = 0

    fd = GetFileHandle(logFile, offset)
    if not fd:
        return []

    lines = []
    for x in range(0, 4096):
        line = fd.readline()
        if line:
            try:
                line = line.decode('gbk')
            except:
                pass
            lines.append(line)
        else:
            break

    if len(lines) == 0:  # 文件读取完毕 判断下一个文件是否生成
        beginIndex = logFile.rfind('_')
        endIndex = logFile.rfind('.')
        count = logFile[beginIndex + 1:endIndex]
        count2 = int(count) + 1
        logFile2 = logFile.replace("_%s." % count, "_%d." % count2)
        if os.path.exists(logFile2):
            sql = "update logstat set needread=0 where filename='%s'" % logFile
            cur.execute(sql)
        DelFileHandle(logFile)
    else:
        offset = fd.tell()
        sql = "update logstat set offset=%d where filename='%s'" % (offset, logFile)
        cur.execute(sql)

    return lines


def GetLogTime(line):
    if len(line) < 19:
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[0:-3]
    line = line[0:19]
    try:
        dt = datetime.datetime.strptime(line, "%Y%m%d|%H%M%S|%f")
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[0:-3]
    except:
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[0:-3]


def PushToServer(localIP, logName, logDir, logFile, lines):
    msglist = []

    for line in lines:
        msgdict = {}
        msgdict["host"] = localIP
        msgdict["logName"] = logName
        msgdict["logPath"] = logDir
        msgdict["logFile"] = logFile
        logTime = GetLogTime(line)
        if logTime:
            msgdict["logtime"] = logTime
        message = line.strip()  # 去除换行
        if message:
            msgdict["message"] = message  
            try:
                msg = json.dumps(msgdict, ensure_ascii=False)
            except Exception as e:
                msg = str(msgdict)

            msglist.append(msg)

    dataSend.dataSendMgr.Send("logstash", '\\n'.join(msglist).encode())
    pass


def Handle(localIP, logName, logDir):
    if not os.path.exists(logDir):
        return

    for logFile in sacnPath(logDir):
        if needRead(logFile) == 0:
            continue

        while True:
            lines = ReadLog(logFile)
            if len(lines) == 0:
                break
            PushToServer(localIP, logName, logDir, logFile, lines)


logCfg = None


def ReadAndSend():
    global logCfg
    if not logCfg or (datetime.datetime.now() - logCfg['lasttime']).seconds > 600:
        tmplogCfg = common.GetCfg("logcfg")
        if tmplogCfg:
            tmplogCfg['lasttime'] = datetime.datetime.now()
            logCfg = tmplogCfg

    if logCfg:
        for cfg in logCfg["LogCfg"]:
            Handle(cfgMgr.localIP, cfg["LogName"], cfg["LogDir"])


def run():
    # 获取日志配置
    logCfg = common.GetCfg("logcfg")
    if not logCfg:
        return None

    beginDT = datetime.datetime.now()
    while True:
        curDT = datetime.datetime.now()
        if (curDT - beginDT).seconds > 600:  # 10分钟更新一次配置
            beginDT = curDT
            tmpLogCfg = common.GetCfg("logcfg")
            if tmpLogCfg:
                logCfg = tmpLogCfg

        for cfg in logCfg["LogCfg"]:
            Handle(cfgMgr.localIP, cfg["LogName"], cfg["LogDir"])

        time.sleep(30)


if __name__ == '__main__':
    import sys

    reload(sys)
    sys.setdefaultencoding('utf-8')

    run()
