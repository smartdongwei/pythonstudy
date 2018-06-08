# -*- coding: utf-8 -*-
import os
import time
import datetime
import cfgMgr


def GetMemInfo():
    memInfo = {}
    stdout = os.popen("cat /proc/meminfo")

    for line in stdout.readlines():
        if len(line.split(':')) != 2:
            continue

        var, value = line.split(':')
        if "MemTotal" == var:
            value, unit = value.split()
            memInfo["PhysicalMem"] = int(int(value) / 1024)

        if "MemFree" == var:
            value, unit = value.split()
            memInfo["PhysicalMemUse"] = int(memInfo["PhysicalMem"] - int(value) / 1024)

        if "SwapTotal" == var:
            value, unit = value.split()
            memInfo["VirtualMem"] = int(int(value) / 1024)

        if "SwapFree" == var:
            value, unit = value.split()
            memInfo["VirtualMemUse"] = int(memInfo["VirtualMem"] - int(value) / 1024)

    return memInfo


def GetUserCount():
    stdout = os.popen("who")
    lines = stdout.readlines()
    return len(lines)


def GetCpuUse():
    stdout1 = os.popen("cat /proc/stat | grep cpu")
    vals1 = stdout1.readline().split()

    time.sleep(1)

    stdout2 = os.popen("cat /proc/stat | grep cpu")
    vals2 = stdout2.readline().split()

    user = int(vals1[1])
    nice = int(vals1[2])
    system = int(vals1[3])
    idle = int(vals1[4])
    iowait = int(vals1[5])
    irq = int(vals1[6])
    softirq = int(vals1[7])

    total1 = user + nice + system + idle + iowait + irq + softirq
    used1 = user + nice + system + irq + softirq

    user = int(vals2[1])
    nice = int(vals2[2])
    system = int(vals2[3])
    idle = int(vals2[4])
    iowait = int(vals2[5])
    irq = int(vals2[6])
    softirq = int(vals2[7])

    total2 = user + nice + system + idle + iowait + irq + softirq
    used2 = user + nice + system + irq + softirq

    return int((used2 - used1) * 100 / (total2 - total1))


def GetDiskArrayStr():
    diskArrayStr = ""
    stdout = os.popen("df -l")
    lineEx = ""
    count = 1
    for line in stdout.readlines():
        if count == 1:
            count = 0
            continue
        line = line.replace("\n", "")
        vars = line.split()
        if len(vars) == 1:
            lineEx = line
            continue
        elif len(vars) == 5:
            lineEx += line
            vars = lineEx.split()
        else:
            pass
        diskArrayStr += vars[5] + ","

    return diskArrayStr[0:-1]


def GetOSStatus():
    osStatus = GetMemInfo()
    osStatus["CpuUse"] = GetCpuUse()
    osStatus["UserCount"] = GetUserCount()
    osStatus["DiskArrayStr"] = GetDiskArrayStr()
    osStatus["ScanTime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    return osStatus


def DiskStatus():
    diskStatusS = []
    stdout = os.popen("df -l")
    lineEx = ""
    count = 1
    for line in stdout.readlines():
        if count == 1:
            count = 0
            continue
        line = line.replace("\n", "")
        vars = line.split()
        if len(vars) == 1:
            lineEx = line
            continue
        elif len(vars) == 5:
            lineEx += line
            vars = lineEx.split()
        else:
            pass

        diskStatus = {}
        diskStatus["TotalSize"] = int(int(vars[1]) / 1024)  # M
        diskStatus["LeaveSize"] = int(int(vars[3]) / 1024)  # M
        diskStatus["SpaceUsed"] = int(vars[4].replace("%", ""))
        diskStatus["DiskName"] = vars[5]
        diskStatus["Status"] = 1
        diskStatus["ScanTime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        diskStatusS.append(diskStatus)

    return diskStatusS


def GetBandWidth(ethName):
    command = "ethtool " + ethName + " | grep Speed "
    stdout = os.popen(command)

    bandWidth = "0"
    try:
        for line in stdout.readlines():
            line = line.replace("\n", "")
            line = line.replace("\t", "")
            line = line[7:]
            bandWidth = line[:len(line) - 4]
            break
    except:
        bandWidth = "0"
        pass

    try:
        bandWidth = int(bandWidth)
    except:
        bandWidth = 0
        pass

    return bandWidth


def GetNetDevItem():
    netDevItemS = []
    stdout1 = os.popen("cat /proc/net/dev ")
    time.sleep(1)
    stdout2 = os.popen("cat /proc/net/dev ")

    lines1 = stdout1.readlines()
    lines2 = stdout2.readlines()

    i = 0
    while i < len(lines1):
        line = lines1[i]
        line2 = lines2[i]
        if line.find(':') == -1:
            i = i + 1
            continue

        # get the interface information and the values
        line = line.replace("\n", "")
        line2 = line2.replace("\n", "")

        dev, values = line.split(':')
        dev1, values2 = line2.split(':')

        dev = dev.strip()

        # ignore information for devices we're not looking for
        # if dev != iface:
        # continue
        values = values.split()
        values2 = values2.split()

        if dev in ('lo', 'sit0'):
            i = i + 1
            continue

        netDevStatus = {}
        netDevStatus["EthName"] = dev
        netDevStatus["RecvByte"] = values[0]
        netDevStatus["SendByte"] = values[8]
        netDevStatus["InPackets"] = values[1]
        netDevStatus["OutPackets"] = values[9]
        netDevStatus["InError"] = values[2]
        netDevStatus["OutError"] = values[10]
        netDevStatus["InLose"] = values[3]
        netDevStatus["OutLose"] = values[11]
        netDevStatus["InVelocity"] = int(values2[0]) - int(values[0])
        netDevStatus["OutVelocity"] = int(values2[8]) - int(values[8])
        netDevStatus["BandWidth"] = GetBandWidth(dev)
        netDevStatus["Status"] = 1
        netDevStatus["ScanTime"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        netDevItemS.append(netDevStatus)
        i = i + 1

    return netDevItemS


def GetServerStatus():
    serverStatusEx = {}
    serverStatusEx["HostIP"] = cfgMgr.localIP

    serverStatus = {}
    serverStatus["OSStatus"] = GetOSStatus()
    serverStatus["DiskStatus"] = DiskStatus()
    serverStatus["NetDevStatus"] = GetNetDevItem()

    serverStatusEx["ServerStatus"] = serverStatus

    return serverStatusEx


if __name__ == '__main__':
    print(GetServerStatus())
