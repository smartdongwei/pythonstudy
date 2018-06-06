# -*- coding:utf-8 -*-

"""
主要用于从ftp中下载ZIP包到/data1/data/WIFIZK_zip，然后解压到/data1/data/WIFIZK/wifizk
ftp://176.100.13.249:8023/vpnin/WIFI/sanhui/
账号 cetcwifi
密码 cetcwifi
"""

from threadpool import makeRequests
import traceback
import json
import threadpool as tp
import time
import os
from ftplib import FTP
import zipfile
import logging
import shutil

scanZipWifi='/data1/data/WIFIZK_zip/'      #zip包存储的位置

scanTxtWifi='/data1/data/WIFIZK/wifizk/'   #数据转换工具的输入目录

logFile='/data1/data/WIFIZK_log/'         #日志的存储位置

CONST_HOST = "**"
CONST_PORT="*****"
CONST_USERNAME = "****"
CONST_PWD = "****"

def connect():  # ftp连接代码
    try:
        ftp = FTP()
        ftp.connect(CONST_HOST, CONST_PORT)
        ftp.login(CONST_USERNAME, CONST_PWD)
        return ftp
    except:
        print "ftp连接错误"


def ftpZip():
    try:
        ftpCn=connect()
        ftpCn.cwd(mustPwd)
        ftpZipFileLists=[mustPwd+'/' + ftpZip for ftpZip in ftpCn.nlst() if os.path.splitext(ftpZip)[1] == '.zip']    #从ftp中下载zip包
        logging.info('本次下载的zip包数量为：'+str(len(ftpZipFileLists)))

        pool = tp.ThreadPool(10)          #多线程模块用于zip包下载和解压
        requests = makeRequests(ftpDowZip2Txt,ftpZipFileLists)
        [pool.putRequest(req) for req in requests]
        pool.wait()

        #for ftpZipFile in ftpZipFileLists:
            #ftpDowZip2Txt(ftpZipFile)

        ftpCn.quit()                    #ftp断开

        txtLists=[scanZipWifi + '/' + txtFile for txtFile in os.listdir(scanZipWifi) if os.path.splitext(txtFile)[1] == '.txt']   #下载的txt存储路径
        logging.info('本次copy的txt包数量为：'+ str(len(txtLists)))
        for txtFile in txtLists:
            txtName=os.path.split(txtFile)[1]                        #获得txt文件名
            shutil.move(txtFile,scanTxtWifi+txtName+'.ing')        #move到输入目录，对其加后缀 .ing
            os.renames(scanTxtWifi+txtName+'.ing',scanTxtWifi+txtName)
    except:
        traceback.print_exc()
        logging.exception('出问题了！！！！！！！！！！！！！！！！！！！！！！！！')


def ftpDowZip2Txt(ftpZipFile):
    try:
        ftpD=connect()                #连接ftp
        ftpD.cwd(mustPwd)
        zipName = ftpZipFile.split('/')[-1]  # zip包名称
        linuxFile=scanZipWifi+zipName    #zip包文件在linux下的存储位置
        with open(linuxFile,'wb') as f1:                 #在ftp上下载文件
            ftpD.retrbinary('RETR ' + zipName, f1.write,1024)

        with zipfile.ZipFile(linuxFile,'r') as zf:         #对zip包进行解压
            for i in zf.namelist():
                zf.extract(i,scanZipWifi)                  #解压zip包保存在   /data1/data/WIFIZK_zip/
        os.remove(linuxFile)                              #删除zip包
        ftpD.delete(zipName)                           #删除ftp上数据
        ftpD.quit()
    except:
        logging.exception(ftpZipFile)



if __name__ == '__main__':
    cProcessNum = os.popen('ps aux|grep -c wifiFtpZipMove.py')  #判断脚本是否正在运行
    if 3 < int(cProcessNum.read()):
        print 'Process already exists'
        exit(0)
 
    mustPwd = '/vpnin/WIFI/sanhui'    #ftp上文件的存储位置
    today = time.strftime('%Y%m%d',time.localtime())
    print today
    logging.basicConfig(filename=logFile + today + '.log',filemode='a', level=logging.NOTSET,format= '%(asctime)s - %(levelname)s : %(message)s')
    ftpZip()      #ftp下载模块
    logging.info('--------------------')  #结束标识符
