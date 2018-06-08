# -*- coding: utf-8 -*-
import json
import time
import datetime
import logMgr
import cfgMgr
import dataSend

import serverStatus
import procStatus
import dirStatus
import linkStatus
import logFileReader


def PushStatus():
    try:
        sStatus = serverStatus.GetServerStatus()
        if sStatus:
            dataSend.dataSendMgr.Send("synitilbase", json.dumps(sStatus, ensure_ascii=False))

        pStatus = procStatus.GetProcStstus()
        if pStatus:
            dataSend.dataSendMgr.Send("synitilbase", json.dumps(pStatus, ensure_ascii=False))

        dStatus = dirStatus.GetDirStstus()
        if dStatus:
            dataSend.dataSendMgr.Send("synitilbase", json.dumps(dStatus, ensure_ascii=False))

        lStatus = linkStatus.GetLinkStstus()
        if lStatus:
            dataSend.dataSendMgr.Send("synitilbase", json.dumps(lStatus, ensure_ascii=False))

    except Exception, e:
        logMgr.log.error("PushStatus Exception:%s" % e)

    pass


if __name__ == '__main__':
    import sys

    reload(sys)
    sys.setdefaultencoding('utf-8')

    cfgMgr.Init()
    dataSend.dataSendMgr.Connect()

    PushStatus()
    count = 0

    btime = datetime.datetime.now()
    while True:
        time.sleep(5)

        ctime = datetime.datetime.now()
        if (ctime - btime).seconds >= 60:
            btime = ctime
            logFileReader.ReadAndSend()  # 一分钟推送一次
            count += 1
            if count == 5:  # 五分钟推送一次
                count = 0
                PushStatus()
