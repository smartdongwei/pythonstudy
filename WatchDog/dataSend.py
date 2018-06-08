# -*- coding: utf-8 -*-
from kafka import SimpleClient, SimpleProducer
import requests
import cfgMgr
import logMgr


class DataSendMgr:
    def __init__(self):
        self.producer = None

    def Connect(self):

        if cfgMgr.dataSendType == 'kafka':
            try:
                client = SimpleClient(cfgMgr.dataSendServer)
                self.producer = SimpleProducer(client, async=True)
            except Exception, e:
                logMgr.log.error("创建kafka生成者失败:%s,Exception%s" % (cfgMgr.dataSendServer, e))
                self.producer = None
                return False

        return True
        pass

    def Send(self, sendpath, senddata):

        if not self.producer:
            self.Connect()

        if self.producer:
            try:
                self.producer.send_messages(sendpath, senddata.encode())
            except:
                if self.Connect():
                    try:
                        self.producer.send_messages(sendpath, senddata.encode())
                    except Exception, e:
                        logMgr.log.error("kakfa send error:%s,Exception%s" % (cfgMgr.dataSendServer, e))

        else:
            try:
                url = "http://%s/%s" % (cfgMgr.dataSendServer, sendpath)
                requests.post(url, data=senddata.encode())
            except Exception, e:
                logMgr.log.error("http post error:%s,Exception%s" % (cfgMgr.dataSendServer, e))


dataSendMgr = DataSendMgr()
