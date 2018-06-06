#coding:utf-8


import os
import math
import oss2
import traceback
from time import sleep
import ftplib
import time
from datetime import datetime
import traceback
from threadpool import makeRequests
import threadpool as tp
import imghdr
import logging
import datetime

today = datetime.datetime.now().strftime('%Y%m%d')
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',datefmt='%a, %d %b %Y %H:%M:%S',filename='/usr/local/bin/express_log/' + today + '.log',filemode='a')

# FTP配置，IP，端口，用户名，密码
FtpArr = [['176.100.13.249','8021','cetcwl','cetcwl']]

# 遍历文件
def EnumFiles(dir, ext=None):
    Files = []
    NeedExtFilter = (ext != None)
    for root,dirs,files in os.walk(dir):
        for filespath in files:
            filepath = os.path.join(root,filespath)
            extersion = os.path.splitext(filepath)[1][1:]
            if NeedExtFilter and extersion in ext:
                Files.append(filepath)
            elif not NeedExtFilter:
                Files.append(filepath)
    return Files

# 汇智FTP
def hzFtp():
    ftp = ftplib.FTP()
    ftp.connect('12.39.109.133','21')
    ftp.login('jdsm','jdsm')
    ftp.set_debuglevel(0)
    return ftp
# 照片上传函数
def UploadPhoto(FtpInfo):
    try:
        #创建oss2:版本bucket对象
        cEndpoint='12.39.121.158'
        cAuth=oss2.Auth("ZUcC5zNLhgmBCp4J","xAJixgt2VKhvxa1LQGuRK7Qke6fEV0")
        cBucket = oss2.Bucket(cAuth, cEndpoint, 'photo-express')
        cFtp = ftplib.FTP()
        cFtp.connect(FtpInfo[0],FtpInfo[1])
        cFtp.login(FtpInfo[2],FtpInfo[3])
        cFtp.set_debuglevel(0)
        remotepath = '/data/txt_express/'
        cDateDir = cFtp.nlst('/%s'%(FtpInfo[4]))
        #查看二级目录/我们需要的数据在 vpnin/wl
        for cSecondLevel in cDateDir:
            #print cForthLevel
            cSFZDir = cFtp.nlst( cSecondLevel)
            #查看三级目录，为身份证文件夹
            for cThirdLevel in cSFZDir:
                #查看四级目录，为文件存放文件夹
                cFileName = cThirdLevel[10:].replace('/','-')
                print cFileName
                logging.info('文件名：' + cFileName)
                cLocalPath = '/data/photo_express/'+cFileName
                cFile = open(cLocalPath,'wb')
                cFtp.retrbinary('RETR ' + cThirdLevel, cFile.write)
                cFile.close()
                try:
                    cFtp.delete(cThirdLevel)
                    print 'delete %s \n' %cThirdLevel
                    logging.info('删除目录：' + cThirdLevel)
                except:
                    os.system('rm -rf %s'%cFile)
                    logging.info('删除失败文件：' + cFile)
                    continue
                isjpeg = False
                if imghdr.what(cLocalPath)=='jpeg':
                    isjpeg = True
                    res = cBucket.put_object_from_file('erwei/'+cFileName, cLocalPath)
                    if 200!=res.status:
                        print res.status
                        logging.info('图片上传失败，失败代码：' + res.status)
                        continue
                    #os.system('rm -rf %s' %cLocalPath)
                    print cFileName+'\tuplaod successfully'
                    logging.info('图片上传成功：' + cFileName + '\tuplaod successfully')
                else:
                    os.system('cp %s %s'%(cLocalPath,remotepath ))
                    logging.info('复制文件到上传目录：cp ' + cLocalPath + ' ' + remotepath)
                #给汇智推送文件的路径
                hzpath = '/data/jdsm/data/'
                os.system('mv %s %s' %(cLocalPath,hzpath))  #把文件转移到给汇智的目录中
                #创建汇智FTP对象
                ftp=hzFtp()
                #照片数据推送汇智FTP
                #fp = None
                #if isjpeg:
                #    fp = open(remotepath + cFileName,'rb')
                #    print '推送：' + remotepath + cFileName
                #else:
                fp = open(hzpath + cFileName,'rb')
                print '推送：' + hzpath + cFileName
                logging.info('给汇智推送文件：' + cFileName)
                try:
                    hzftppath = './data/'+ os.path.basename(cLocalPath)
                    print 'hzftppath: '+hzftppath
                    ftp.storbinary('STOR '+hzftppath,fp)
                    fp.close()
                    print '给汇智推送成功'
                    logging.info('给汇智推送成功')
                    ftp.close()
                except:
                    fp.close()
                    print '给汇智推送失败'
                    logging.info('给汇智推送失败')
                    logging.info(traceback.print_exc())
                    ftp.close()
                    continue
        cFtp.close()
    except:
        print "FTP信息:%s\t%s"%(FtpInfo[0], FtpInfo[4])
        logging.info(traceback.print_exc())

def Init(FileArr):
    #遍历中间目录文件
    cEndpoint='12.39.121.158'
    cAuth = oss2.Auth("ZUcC5zNLhgmBCp4J","xAJixgt2VKhvxa1LQGuRK7Qke6fEV0")
    cBucket = oss2.Bucket(cAuth, cEndpoint, 'photo-express')
    #上传文件
    for File in FileArr:
        #print os.path.basename(File)
        res = cBucket.put_object_from_file('erwei/'+os.path.basename(File), File)
        if 200!=res.status:
            print res.status
            os.system('rm -rf %s'%File)
            continue
        os.system('rm -rf %s'%File)

#数组分割
def Chunks(FileArr, nBlock):
    n = int(math.ceil(len(FileArr)/float(nBlock)))
    return [FileArr[i:i+n] for i in range(0, len(FileArr), n)]

def ServerStart(FtpInfo):
    DeviceInfo = []
    cFtp = ftplib.FTP()
    print FtpInfo[0]
    cFtp.connect(FtpInfo[0], FtpInfo[1])
    cFtp.login(FtpInfo[2], FtpInfo[3])
    cFtp.set_debuglevel(0)
    cFtp.cwd('/')
    cRootDir = cFtp.nlst()
    #cFtp.close()
    for cDeviceDir in cRootDir:
        if cDeviceDir[0:5]=='vpnin':
            DeviceInfo.append([FtpInfo[0],FtpInfo[1],FtpInfo[2],FtpInfo[3],cDeviceDir])
    cFtp.close()
    #创建线程池
    cPool = tp.ThreadPool(4)
    
    #启动线程
    cRequests = makeRequests(UploadPhoto, DeviceInfo)
    [cPool.putRequest(req) for req in cRequests]
    cPool.wait()


#主函数入口
if __name__ == '__main__':
    #判断程序是否正在运行
    cProcessNum = os.popen('ps aux|grep -c photo_downup.py')
    if 3 < int(cProcessNum.read()):
        print 'Process already exists'
        exit(0)
    try:
        FileArr = EnumFiles('/data/photo_express/')
        fasize = len(FileArr)
        if 0<fasize:
            print fasize
            if 16>fasize:
                FilePart = Chunks(FileArr, fasize)
                pool = tp.ThreadPool(fasize)
            else:
                FilePart = Chunks(FileArr, 16)
                pool = tp.ThreadPool(16)
            requests = makeRequests(Init, FilePart)
            [pool.putRequest(req) for req in requests]
            pool.wait()
    except:
        traceback.print_exc()
    #计算FTP数目，即为开启线程数目
    print '开启%d个线程'%len(FtpArr)
    #创建线程池
    pool = tp.ThreadPool(len(FtpArr))
    #启动线程
    requests = makeRequests(ServerStart, FtpArr)
    [pool.putRequest(req) for req in requests]
    pool.wait()

