# -*- coding: utf-8 -*-

import os
import datetime
import common
import cfgMgr


def __GetDirStstus__(dirName):
    status = {}
    try:
        cmd = "du -sm %s" % dirName
        stdout = os.popen(cmd)
        status["DirSize"] = stdout.readline().split()[0]

        cmd = "find %s -type d |wc -l" % dirName
        stdout = os.popen(cmd)
        status["DirCount"] = stdout.readline().split()[0]

        cmd = "find %s -type f |wc -l" % dirName
        stdout = os.popen(cmd)
        status["FileCount"] = stdout.readline().split()[0]

        return status
    except Exception, e:
        print("GetDirStstus error cmd:%s Exception:%s" % (cmd, e))
        return None

    pass

def GetDirStstus():
    # 获取本机配置信息
    dirCfg = common.GetCfg('dircfg')
    if not dirCfg:
        return None

    if len(dirCfg["DirCfg"]) == 0:
        return None

    dirStatus = []
    for cfg in dirCfg["DirCfg"]:
        status = __GetDirStstus__(cfg["DirName"])
        if not status:
            status={}
            status["Status"] = 0
        else:
            status["Status"] = 1

        status["ID"] = cfg["ID"]
        status["DirName"] = cfg["DirName"]
        status["ScanTime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        dirStatus.append(status)

    dirStatusDict = {}
    dirStatusDict["HostIP"] = cfgMgr.localIP
    dirStatusDict["DirStatus"] = dirStatus
    return dirStatusDict

    pass


if __name__ == '__main__':
    import sys

    reload(sys)
    sys.setdefaultencoding('utf-8')

    cfgMgr.Init()

    print(GetDirStstus())
