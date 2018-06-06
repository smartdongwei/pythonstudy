# -*- coding:utf-8 -*-
import traceback
import socket
import json
import time
import datetime
import os
from ftplib import FTP
import logging
import re
import codecs
import sys
import threadpool as tp
from threadpool import makeRequests
from odps import ODPS

date_arr = []
ExpressArr = ['海康动态人脸', 'hk_dong_tai_ren_lian_new_sc']
fileOld="old.txt"                      #存储已下载文件夹的txt
fileDownload='/data/dongTaiRenLian/txtAll/'                  #ftp下载文件的存储路径
CONST_HOST="12.39.109.133"
CONST_PORT="21"
CONST_USERNAME="sanhui"
CONST_PWD="sanhui"
ftpRootPath='/data/'                                      #ftp根目录
runLog='/data/dongTaiRenLian/log/'                         #日志的存储目录
cOdps = ODPS('ZUcC5zNLhgmBCp4J', 'xAJixgt2VKhvxa1LQGuRK7Qke6fEV0', 'kunlun',
             endpoint='http://service.cn-xinjiang-xjkl-d01.odps.xjtz.xj/api')


def connect():  # ftp连接代码
    try:
        ftp = FTP()
        ftp.connect(CONST_HOST, CONST_PORT)
        ftp.login(CONST_USERNAME, CONST_PWD)
        return ftp
    except:
        print "ftp连接错误"
        logging.exception("ftp连接错误")


def oldFile():  # 返回需要下载的文件夹名称
    oldFileLists = []
    with open(fileReal+'/'+fileOld, "rb") as f:
        for file in f.readlines():
            oldFileLists.append(file.strip())

    ftp3=connect()
    ftp3.cwd(ftpRootPath)    
    ftpTimeList=ftp3.nlst()    #获得第二层目录下所有时间文件名
    #ftpTimeList=['20171102']
    try:              # 保证如果历史文件夹为空时不会出
        newFileLists = sorted(filter(lambda x: x not in oldFileLists, ftpTimeList))
        ftpTimeList.sort()
        newFileLists.append(ftpTimeList[-1])  # 获得没有下载的完的文件夹名称
    except:
        newFileLists = sorted(filter(lambda x: x not in oldFileLists, ftpTimeList))
        
    ftp3.quit()
    return newFileLists

'''
def judgMent(fileMessage):
    print fileMessage
    fileMessageList=filter(None,fileMessage.split(" "))
    txtName=fileMessageList[-1]             #  获得文件的名称    snap_20171105_235901_0.txt
    txtTime = fileMessageList[5:8]  # 获得文件的修改时间    ['Nov', '05', '16:05','2017']
    txtTime.append(txtName.split("_")[1][0:4])
    timeStamp=time.mktime(time.strptime(' '.join(txtTime), "%b %d %H:%M %Y"))   #把文件修改时间转为时间戳格式
    if timeStamp < timeStampNow:
        return txtName
'''

def judgMentYN(timeTxtList):   #通过时间，判断那些txt文档是增量的，需要下载
    timeTxt=''.join(timeTxtList)
    txtTimeStruct=time.strptime(timeTxt,"%Y%m%d%H%M%S")
    if txtTimeStruct > timeStampNow:   
        return True
    else:
        return False
    
    
def dowloadTxt(txtFileName):
    try:
        ftp2=connect()         #连接ftp
        fileDownloads=fileDownload+txtFileName.split('/')[2]+"/"+txtFileName.split('/')[3]   #txt文件本机的存放位置 /data/dongTaiRenLian/txtAll/20171106/snap_20171106_000120_0.txt
        with open(fileDownloads, 'wb') as f:  # 把txt文档下载到指定的缓存目录下
            ftp2.retrbinary('RETR ' + txtFileName, f.write, 1024)
            
        with open(fileDownloads,'r') as f1:
            a=f1.read()                                                 #读取下载文档的内容
        try:
            aa=unicode(a,'gb2312')                                         #如果txt是ANSI编码，将内容转码为unicode
            with codecs.open(fileDownloads,'w',encoding="utf-8") as f2:    # 使用utf-8格式保存文档   
                f2.write(aa)
            logging.info(fileDownloads+'需要改成utf-8格式')
        except:
            print 'buxuyao'

        ftp2.quit()
    except:
        traceback.print_exc()
        logging.exception(txtFileName)
    

def enum_files(dir):  # 获取某个文件夹中所有文件
    file_arr = []
    for root, dirs, files in os.walk(dir):
        for filespath in files:
            if judgMentYN(filespath.split('_')[1:3])==True:
                filepath = os.path.join(root, filespath) 
                file_arr.append(filepath)
    return file_arr


def downl2ftp(newFileLists):
    
    for fileTime in newFileLists:
        ftp1=connect()
        logging.info(fileTime)
        try:
            if os.path.exists(fileDownload+fileTime)==False:
                os.mkdir(fileDownload+fileTime)
            
            ftp1.cwd(ftpRootPath+fileTime)
            txtNamesList=[txtName for txtName in ftp1.nlst() if judgMentYN(txtName.split('_')[1:3])==True]
            logging.info(fileTime+"需要下载数量为："+str(len(txtNamesList)))
                
            txtFileNameList=[ftpRootPath+fileTime+'/'+txtName for txtName in txtNamesList]    # /data/20171106/snap_20171106_000120_0.txt
          
            timeEnd=max([fileTime+txtName.split('_')[2] for txtName in txtNamesList])    #获得最后的文件夹
            try:   #  多线程模块，如果可以用，最好用这个
                pool = tp.ThreadPool(10)
                requests = makeRequests(dowloadTxt, txtFileNameList)
                [pool.putRequest(req) for req in requests]
                pool.wait()
            except:
                traceback.print_exc()
                
            with open(fileReal+'/'+fileOld, "a+") as f:
                f.writelines(fileTime)
                f.writelines("\n")

            with open(fileReal+'/'+'oldRuntime.txt','w') as f:
                f.write(timeEnd)      
            ftp1.quit()               
        except:
            traceback.print_exc()
            logging.exception('error')
    
    

def Lib2Odps(file_path):
    # 从txt文档中获取数据，再写入到
    print file_path
    logging.info(file_path)
    try:
        os.system('odpscmd -e "tunnel upload %s kunlun.%s/dt=%s -c \'%s\' -fd \'%s\' -rd \'%s\' -dbr true -s false"' % (file_path, ExpressArr[1], partition, 'utf-8', ';','\\r\\n'))
    except:
        print file_path+"odps上传错误"
        logging.info('errr'+file_path) 
        

        
def uploadOdps(newFileLists):     #上传到odps
    for newFile in newFileLists:
        file_arrs = enum_files(fileDownload+newFile)   #读取所有需要上传的txt文件
        print len(file_arrs)
        logging.info(newFile+'需要上传文件数量为：'+str(len(file_arrs)))
        try:  # 上传时的模块

            pool11 = tp.ThreadPool(10)
            requests11 = makeRequests(Lib2Odps,file_arrs)
            [pool11.putRequest(req) for req in requests11]
            pool11.wait()
            logging.info("所有文件上传结束")
        except:
            logging.exception("上传过程中出错")
        
    

if __name__  == "__main__":
    partition = time.strftime("%Y%m%d", time.localtime())
    logging.basicConfig(filename=runLog+'dongtaiRL'+partition+".log",filemode='a',level=logging.NOTSET,format="%(asctime)s - %(levelname)s: %(message)s")  #日志的输出格式
    table_obj = cOdps.get_table(ExpressArr[1], 'kunlun')
    try:
        # 判断是否存在数组中
        if partition not in date_arr:
            print '分区%s' % partition
            table_obj.create_partition('dt=' + partition, if_not_exists=True)  # 创建分区
            date_arr.append(partition)
            print table_obj
    except:
        print '创建分区错误'

    try:
                        
        fileReal = os.path.split(os.path.realpath(sys.argv[0]))[0]  # 获取脚本所在的文件路径
        print fileReal
        with open(fileReal+'/'+'oldRuntime.txt','r') as f:
            timeOld=f.read().strip()
        #timeOld="20170101111111"
        print timeOld
        timeStampNow=time.strptime(str(timeOld),"%Y%m%d%H%M%S")    
        print timeStampNow
        newFileLists=oldFile()          #返回之前已经下载过的文件夹名称
        print newFileLists
        #newFileLists=os.listdir(fileDownload)
        print newFileLists
        downl2ftp(newFileLists)
        uploadOdps(newFileLists)

    except:
        traceback.print_exc()
        logging.exception('error')

