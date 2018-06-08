# -*- coding: utf-8 -*-

import ConfigParser as cfgparser

watchdogServer = "127.0.0.1:8088"
localIP = "127.0.0.1"
dataSendType = "http"
dataSendServer = "127.0.0.1:9092"



def Init():
    global watchdogServer, localIP, dataSendType, dataSendServer

    cfg = cfgparser.ConfigParser()
    cfg.read("cfg.ini")
    localIP = cfg.get('default', 'localIP')
    watchdogServer = cfg.get('default', 'watchdogServer')
    dataSendType = cfg.get('default', 'dataSendType')
    dataSendServer = cfg.get('default', 'dataSendServer')


    pass
