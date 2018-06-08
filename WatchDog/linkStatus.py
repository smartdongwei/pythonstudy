# -*- coding: utf-8 -*-

import os
import socket
import datetime
import common
import cfgMgr


def __Ping__(CheckIP):
    beginDT = datetime.datetime.now()
    os.system("./ping %s -c 3 > /dev/null" % CheckIP)
    dtSpace = datetime.datetime.now() - beginDT
    if dtSpace.seconds > 4:  # 超过5秒钟 算检测失败
        status = 0
    else:
        status = 1

    return status
    pass


def __Telnet__(CheckIP, CheckPort):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ret = client.connect_ex((CheckIP, CheckPort))

    if ret == 0:
        client.close()
        return 1

    return 0
    pass


def GetLinkStstus():

    # 获取本机配置信息
    linkCfg = common.GetCfg('linkcfg')
    if not linkCfg:
        return None

    if len(linkCfg["LinkCfg"]) == 0:
        return None

    linkStatus = []
    for cfg in linkCfg["LinkCfg"]:
        linkType = cfg["LinkType"]
        status = {}
        status["ID"] = cfg["ID"]
        status["LinkName"] = cfg["LinkName"]
        if linkType == 1:  # ping
            status["Status"] = __Ping__(cfg["CheckIP"])
        elif linkType == 2:  # telnet
            status["Status"] = __Telnet__(cfg["CheckIP"], cfg["CheckPort"])

        status["ScanTime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        linkStatus.append(status)

    linkStatusDict = {}
    linkStatusDict["HostIP"] = cfgMgr.localIP
    linkStatusDict["LinkStatus"] = linkStatus

    return linkStatusDict


if __name__ == '__main__':
    import sys

    reload(sys)
    sys.setdefaultencoding('utf-8')

    cfgMgr.Init()
    print(GetLinkStstus())
