# -*- coding: utf-8 -*-
import json
import requests

import cfgMgr
import logMgr


def GetCfg(cfgType):
    cfgServer = cfgMgr.watchdogServer
    localIP = cfgMgr.localIP

    content = {}
    content['hostip'] = localIP

    url = "http://%s/%s" % (cfgServer, cfgType)
    try:
        ret = requests.get(url, params=content, timeout=5)
        if ret.status_code == 200:
            return json.loads(ret.text)
        else:
            logMgr.log.error("http get error status_code:%d Exception:%s" % (url, ret.status_code))
    except Exception, e:
        logMgr.log.error("get error url:%s Exception:%s" % (url, e))
    return None
