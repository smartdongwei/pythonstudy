# -*- coding: utf-8 -*-

import os
import datetime
import common
import cfgMgr


def GetProcPID(procItem):
    # 获取进程PID
    if procItem["ProcType"] == 1:
        cmd = "pidof %s" % procItem["ProcFile"]
        stdout = os.popen(cmd)
        pidArray = stdout.read().split()
        return pidArray
        pass
    elif procItem["ProcType"] == 2:
        cmd = "service %s status" % procItem["ProcFile"]
        stdout = os.popen(cmd)
        out = stdout.read()
        beginindex = out.find("(")
        endindex = out.find(")")
        if beginindex < 0 or endindex < 0:
            return []
        out = out[beginindex + 1:endindex]

        out = out.replace('pid', '')
        pid = out.strip()
        return [pid, ]
        pass
    elif procItem["ProcType"] == 3:
        pidList = []
        cmd = "netstat -l -p -t --numeric-hosts --numeric-ports"
        stdout = os.popen(cmd)

        for line in stdout.readlines():
            lineArray = line.split()
            if lineArray[0] != "tcp" and lineArray[0] != "udp":
                continue
            localAddr = lineArray[3].split(":")
            localPort = localAddr[len(localAddr) - 1]
            if localPort == procItem["CheckPort"]:
                pid = lineArray[6].split("/")[0]
                if pid not in pidList:
                    pidList.append(pid)

        return pidList
    return None


def __GetProcStstus__(pid):
    procStstus = {}
    procStstus["Pid"] = pid
    procStstus["Status"] = "1"
    procStstus["ScanTime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    cmd = "cat /proc/%s/status" % pid
    stdout = os.popen(cmd)
    for line in stdout.readlines():
        lineArray = line.split(":")
        if lineArray[0] == "VmRSS":
            procStstus["MemUse"] = int(int(lineArray[1].split()[0]) / 1024)
            if procStstus["MemUse"] == 0:
                procStstus["MemUse"] = 1
        elif lineArray[0] == "Threads":
            procStstus["ThreadNumber"] = lineArray[1].split()[0]

    # 句柄数
    cmd = "ls /proc/%s/fd/ | wc -l" % pid
    stdout = os.popen(cmd)
    procStstus["HandleNumber"] = stdout.readline().split()[0]

    # socket 数
    cmd = "file /proc/%s/fd/* | grep socket | wc -l" % pid
    stdout = os.popen(cmd)
    procStstus["NetworkNumber"] = stdout.readline().split()[0]

    # cpu使用率
    cmd = "top -p %s -b -n 1 |grep %s" % (pid, pid)
    stdout = os.popen(cmd)
    lines = stdout.readlines()
    procStstus["CpuUse"] = lines[-1].split()[8]

    return procStstus

    pass


def GetProcStstus():
    procCfg = common.GetCfg('proccfg')
    if not procCfg:
        return None

    if len(procCfg["ProcCfg"]) == 0:
        return None

    ProcStatusDict = {}
    ProcStatus = []
    for procInfo in procCfg["ProcCfg"]:
        pidList = GetProcPID(procInfo)
        if len(pidList) > 0:
            for pid in pidList:
                status = __GetProcStstus__(pid)
                status["ID"] = procInfo["ID"]
                status["ProcName"] = procInfo["ProcName"]
                status["Status"] = 1
                status["ScanTime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ProcStatus.append(status)
        else:
            status = {}
            status["ID"] = procInfo["ID"]
            status["ProcName"] = procInfo["ProcName"]
            status["Status"] = 0
            status["ScanTime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ProcStatus.append(status)

    ProcStatusDict["HostIP"] = cfgMgr.localIP
    ProcStatusDict["ProcStatus"] = ProcStatus

    return ProcStatusDict

    pass


if __name__ == '__main__':
    import sys

    reload(sys)
    sys.setdefaultencoding('utf-8')

    cfgMgr.Init()

    print(GetProcStstus())
