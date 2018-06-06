# coding: utf-8
'''
用于删除日志   日志只保留15天
/data/dongTaiRenLian/log    为12.39.140.72上动态人脸的日志
/root/kuaiDiLog               为快递实名数据的上传odps脚本日志
/usr/local/bin/express_log     为快递实名数据ftp下载的日志
/data/weiZhiShuJu/Log          为实时数据ftp下载和上传脚本log
/data/dongTaiRenLian/txtAll/  动态人脸中下载的txt文档
'''

import os
import time
import traceback
import shutil

def delLog(logFile):
    for logNeedFile in os.listdir(logFile):
        try:
            logfilePath=logFile+'/'+logNeedFile   #获得文件的路径
            #print logfilePath
            fileTime=time.mktime(time.localtime(os.path.getmtime(logfilePath)))    #获得文件的修改时间
            if int(openJbTime - fileTime) >1296000:    #日志只保留10天
                os.remove(logfilePath)
                print logfilePath
        except:
            traceback.print_exc()
            
def delTxt(txtFile):
    for i in os.listdir(txtFile):
        try:
            print i
            fileTime=time.mktime(time.localtime(os.path.getmtime(txtFile+i)))
            if int(openJbTime-fileTime) > 1296000:
                shutil.rmtree(txtFile+i)
                print txtFile+i
        except:
            traceback.print_exc()

def delFile(filePath):
    pass

if __name__ == '__main__':
    
    logFiles=['/data/weiZhiShuJu/Log','/data/dongTaiRenLian/log','/root/kuaiDiLog','/usr/local/bin/express_log','/data/shenDunFangKe/log']         #log日志所存在的根目录，
    openJbTime=time.mktime(time.localtime())   #获得脚本运行的时间，用时间戳格式
    for logFile in logFiles:
        print logFile
        delLog(logFile)    #删除日志
    txtFiles=['/data/dongTaiRenLian/txtAll/','/data/shenDunFangKe/Bak/','/data/shenDunFangKe/Error/']  #动态人脸数据下载存储位置
    for txtFile in txtFiles:
        print txtFile
        delTxt(txtFile)
