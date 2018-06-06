#coding=utf-8
from ctypes import *
import sys
import ftplib
import os,os.path
import zipfile
import logging
from datetime import datetime
import shutil
import time

FTPIP = '176.100.13.76'  # ftp地址
UserName = 'sanhuidownload'  # ftp用户名
PassWord = 'sm1mX4'  # ftp密码
FTPGetDirPath = r'/var/ftp/pub/visiter/'  # 从ftp上获取文件所在的路径
LocalDirPath = '/data/shenDunFangKe/File/'  # 本地暂存路径
UnZipDirPath = '/data/shenDunFangKe/File/'  # 解压出的文件存放的路径
runLog = "/data/shenDunFangKe/log/"
ERRORFILE = "/data/shenDunFangKe/Error/"     #报错文件存储位置
DATEFILEBAK = "/data/shenDunFangKe/Bak/"                       # 备份文档的存储位置

class MyFTP:
    ftp=ftplib.FTP()
    bIsDir=False
    path=""

    def __init__(self,host,port=21):              #初始化时传入ip地址，判断端口是否需要更改
        # self.ftp.set_debuglevel(2)
        # self.ftp.set_pasv(0)
        self.ftp.connect(host,port)

    def login(self,user,password):
        self.ftp.login(user,password)
        print self.ftp.welcome

    def DownloadFile(self,LocalFile,RemoteFile):
        file_handleer=open(LocalFile,'wb')
        #print file_handleer
        self.ftp.retrbinary('RETR %s'%(RemoteFile),file_handleer.write)
        file_handleer.close()
        return True

    def DownloadFileTree(self,LocalDir,RemoteDir=None):
        if os.path.isdir(LocalDir)==False:
            os.makedirs(LocalDir)                 #判断是否存在本地存储目录，没有则新加
        if RemoteDir==None:
            pass
        else:
            self.ftp.cwd(RemoteDir)
        RemoteNames=self.ftp.nlst()
        print RemoteNames
        for detile in RemoteNames:
            if detile[-3:] =="zip":       #遍历目标文件夹，判断是否为zip
                Local=os.path.join(LocalDir,detile)
                if self.isDir(detile):
                    self.DownloadFileTree(Local,detile)  #若为文件夹，递归调用本方法
                else :
                    self.DownloadFile(Local,detile)     #若为文件，则执行该方法下载
            self.ftp.delete(RemoteDir + "/" + detile)
            print RemoteDir + "/" + detile
        self.ftp.cwd("..")
        return 0

    def show(self,list):
        result=list.lower().split(" ")
        if self.path in result and " "in result:
            self.bIsDir=True

    def isDir(self,path):
        self.bIsDir=False
        self.path=path
        self.ftp.retrlines('LIST',self.show)
        return self.bIsDir

    def close(self):
        self.ftp.quit()


class ZipToNormal:

    def unzip_file(self,zipfildir, unziptodir):
        if not os.path.exists(unziptodir):os.mkdir(unziptodir, 0777)
        if not os.path.exists(zipfildir):os.mkdir(zipfildir,0777)
        names = [name for name in os.listdir(zipfildir) if os.path.isfile(os.path.join(zipfildir, name))]
        for zipfilename in names:
            print zipfilename
            logging.info(zipfilename)
            try:
                zfobj = zipfile.ZipFile(zipfildir + zipfilename)
                for name in zfobj.namelist():
                    zfobj.extract(name, zipfildir)
                shutil.move(zipfildir+zipfilename,dataFileBakToday)
            except:
                print "UnZip ERROR: -- %s -- can not unzip" % zipfilename
                shutil.move(zipfildir + zipfilename, dataFileBakToday)
                logging.error("FileName: -- %s --  can not unzip" % zipfilename)


if __name__ == "__main__":
    today = time.strftime("%Y%m%d", time.localtime())
    dataFileBakToday = DATEFILEBAK  + today
    if os.path.exists(dataFileBakToday) == False:      #判断备份目录是否存在，不存在则创建这个目录
        os.mkdir(dataFileBakToday)

    logging.basicConfig(
        filename=runLog + today + "_ftpDowln.log",
        level=logging.ERROR,
        filemode='a',
        format='%(levelname)s:%(asctime)s:%(message)s',
    )
    ftp = MyFTP(FTPIP)  # 默认端口21
    ftp.login(UserName, PassWord)
    ftp.DownloadFileTree(LocalDirPath,FTPGetDirPath)  # 第一个参数是本地存储目录，第二个参数是目标文件所在的目录,目标文件夹路径默认为None，需要时更改
    ftp.close()

    unzip = ZipToNormal()
    unzip.unzip_file(LocalDirPath, UnZipDirPath)
    logging.info('------')
