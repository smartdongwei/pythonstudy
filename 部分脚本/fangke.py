# coding:utf-8
"""
从ftp中下载访客数据zip包，解压后得到一个txt，两张图片，图片上传到oss txt上传到odps
"""
from odps import ODPS
import traceback
import json
import threadpool as tp
import time
import datetime
import os
import ftplib
import logging
import random
import sys
import codecs
import zipfile
import re
import random
reload(sys)
import shutil
import oss2
from threadpool import makeRequests
sys.setdefaultencoding('utf-8')
FTPIP = "176.100.13.76"
FTPNAME = "sanhuidownload"
FTPPASSWD = "sm1mX4"
ZIPDOWNPATH = "/data/shenDunFangKe/File/"                      # zip包下载位置以及解压位置
DATEFILEBAK = "/data/shenDunFangKe/Bak/"                       # 备份文档的存储位置
ERRORFILE = "/data/shenDunFangKe/Error/"                       #报错之后文档的存储位置
photoFileGu = "http://oss-cn-xinjiang-xjkl-d01-a.xjtz.xj/shujumen/fangke/"    #照片存储在oss时的固定路径
runLog = "/data/shenDunFangKe/log/"                             #日志的存放位置

def txt_Jpg_Upload(filePath):
    """
    :param file: 是解压后的文件存储的位置
    :return:
    """
    try:
        txtAllPath = []     #获得该文件夹下所有 txt 文档的路径
        jpgAllPath = []     #获得该文件夹下所有 jpg 文档的路径
        for root, dirs, files in os.walk(filePath):      #获取照片,txt在本地的路径
            for file in files:
                if os.path.splitext(file)[1] == '.txt':
                    txtAllPath.append(root + '/' + file)
                else:
                    jpgAllPath.append(root + '/' + file)
        logging.info("本次上传txt文档共有%s个"%str(len(txtAllPath)))
        logging.info("本次上传照片共有%s个" % str(len(jpgAllPath)))

        #for txtPath in txtAllPath:
            #txt_Upload_Odps(txtPath)               #用于txt文档上传到odps   最后加多线程
        try:
            pool = tp.ThreadPool(5)
            requests = makeRequests(txt_Upload_Odps, txtAllPath)
            [pool.putRequest(req) for req in requests]
            pool.wait()
            logging.info('所有文件下载完毕'.decode('utf-8'))
        except:
            traceback.print_exc()
        
        jpg_Upload_Oss(jpgAllPath, filePath)      #  用于照片上传到oss
       
        for i in os.listdir(ZIPDOWNPATH):         #删除解压路径中的文件夹
            try:
                shutil.rmtree(ZIPDOWNPATH+i)
                logging.info(i)         
            except:
                traceback.print_exc()
        
    except:
        traceback.print_exc()
        logging.exception()


def txt_Upload_Odps(txtPath):
    """
    :param txtPath:  txt文档的存储位置，是一个字符串
    :return:    将txt文档上传到odps
    """
    print txtPath
    logging.info(txtPath)
    try:
        with open(txtPath, 'r') as f:
            for strFuck in f.readlines()[1:]:       #因为第一行是标题，所以读取数据时去除第一行
                try:
                    dataList = strFuck.split("\t")
                    if dataList[2] != '':
                        dataList[2] = photoFileGu + dataList[2]     #现场照片的图片路径回填
                    if dataList[13] !='':
                        dataList[13] = photoFileGu + dataList[13]  # 身份证照片的图片路径回填
                    
                    with table_obj.open_writer(partition = 'dt='+today) as writer:
                        writer.write(dataList)                  #把每一行数据写入到odps
                    
                    print dataList
                except:
                    traceback.print_exc()
                    print dataList[2]             #输出错误信息
    except:
        traceback.print_exc()


def jpg_Upload_Oss(jpgPathList,filePath):
    """
    :param jpgPath:   图片的存储路径 列表形式
    :return:  把图片上传到oss中
    """
    cEndpoint = '12.39.121.158'
    cAuth = oss2.Auth("ZUcC5zNLhgmBCp4J", "xAJixgt2VKhvxa1LQGuRK7Qke6fEV0")
    cBucket = oss2.Bucket(cAuth, cEndpoint, 'shujumen')
    for jpgPath in jpgPathList:
        logging.info(jpgPath)
        print jpgPath 
        flag = 0
        try:
            cFileName = jpgPath.split(filePath)[1]     #如果jpg文件路径是 /data/someFile/1520446190/201701/kkk.jpg   cFileName:1520446190/201701/kkk.jpg
            jpgFileName = cFileName.split('/')       #如果 20180306/201701/kkk.jpg   切分成 ['1520446190', '201701', 'kkk..jpg']
            if len(jpgFileName) == 2:
                fileName = ''.join(jpgFileName[1])        #  kkk.jpg
            else:
                fileName = '/'.join(jpgFileName[1:])    #  201701/kkk.jpg
                photoFileName = jpgFileName[1]           #   201701
                suiBianMingZiError = errorFileToday + '/'+ photoFileName    #  /data/shenDunFangKe/Error/201701
                if os.path.exists(suiBianMingZiError) == False:    #创建error目录
                    os.mkdir(suiBianMingZiError)
                flag=1
            
            res = cBucket.put_object_from_file('fangke/'+fileName, jpgPath)    # 'fangke/'+cFileName  oss项目的存储路径   jpgPath： 照片的本地路径
            if res.status != 200:      #如果报错，直接把这个文件夹移到错误目录下
                print res.status
                print '失败'
                print jpgPath
                logging.info('图片%s上传失败'%jpgPath)
                if flag ==1:
                    shutil.move(jpgPath, suiBianMingZiError)
                else:
                    shutil.move(jpgPath, errorFileToday)
        except:
            traceback.print_exc()
            logging.exception(jpgPath)
            try:
                shutil.move(jpgPath, errorFileToday)
            except:
                pass



if __name__ == '__main__':
    tableName = "ut_dyzhwyfksj"   #odps表名
    today = time.strftime("%Y%m%d",time.localtime())
    cOdps = ODPS('ZUcC5zNLhgmBCp4J', 'xAJixgt2VKhvxa1LQGuRK7Qke6fEV0', 'kunlun',
                 endpoint='http://service.cn-xinjiang-xjkl-d01.odps.xjtz.xj/api')
    table_obj = cOdps.get_table(tableName, 'kunlun')
    try:
        table_obj.create_partition('dt=' + today, if_not_exists=True)  # 创建分区
    except:
        print '创建分区错误'
    logging.basicConfig(filename=runLog + today + ".log", filemode='a', level=logging.NOTSET,
                        format="%(asctime)s - %(levelname)s: %(message)s")  # 日志的输出格式
    errorFileToday = ERRORFILE + today
    if os.path.exists(errorFileToday) == False:       #判断错误文件存储目录是否存在，不存在则创建这个目录
        os.mkdir(errorFileToday)


    txt_Jpg_Upload(ZIPDOWNPATH)                          #调用上传到odps oss的函数
    logging.info('-----------------------------')          #判断脚本运行结束没

