# coding:utf-8
#  /user/bin/python2.7
"""
1:在congig.ini中设置输入的根目录，该目录下还有一层数据目录 ok
2：输出目录，自己创建FTP目录，要求上传到ftp后文件按照源目录结构存储数据文件 ok
3：在config.ini中配置指定后缀，上传过程中先添加ing后缀  ok
4：实效性，进行特定文件判断，在脚本正在运行时不进行下一次的运行 ok
5：统计，统计每天的数据文件数和数据量
7：备份，设置备份目录，在config.ini中设置选项 ok
有啥问题找他------负责人：胡平安 对，就是nage三汇数据接入组最帅的那个
oh my god
Srce me boil , Lice
"""
import ConfigParser
#import configparser
import time
import logging
from ftplib import FTP
import traceback
import os
bufsize = 1024
import shutil
from threadpool import makeRequests
import threadpool as tp

def connect():
    '''
    连接ftp
    '''
    try:
        ftp = FTP()
        ftp.connect(FTP_HOST, FTP_PORT)
        ftp.login(FTP_USERNAME, FTP_PASSWD)
        return ftp
    except:
        traceback.print_exc()
        logging.exception('ftp connection error')



def EnumFiles(dirPath, type=None):
    '''获取一个文件夹内所有指定后缀文件的列表
    :param dir:目录地址
    :param ext: 后缀名
    '''
    Files = []
    NeedExtFilter = (type != None)
    for root,dirs,files in os.walk(dirPath):
        for filespath in files:
            filepath = os.path.join(root,filespath)
            extersion = os.path.splitext(filepath)[1][1:]
            if NeedExtFilter and extersion in type:
                Files.append(filepath)
            elif not NeedExtFilter:
                Files.append(filepath)
    return Files


def UploadFtp(twoFolde):
    '''把指定后缀的文件上传到ftp
    :param twoFolde:   第二层目录的目录名
    '''
    #连接指定的ftp,cwd到ftp上需要存储的根目录,如果根目录和二级目录不存在则创建该目录,然后切换到该目录下
    ftpConn = connect()
    try:
        ftpConn.cwd(FTP_ROOT_PATH)
    except:
        ftpConn.mkd(FTP_ROOT_PATH)
        ftpConn.cwd(FTP_ROOT_PATH)

    try:
        ftpConn.cwd(twoFolde)
    except:
        ftpConn.mkd(twoFolde)
        ftpConn.cwd(twoFolde)
    #获取第二层目录下所有指定后缀文件路径的列表，然后把文件上传到指定的ftp中
    needUploadPathList = EnumFiles(os.path.join(fileRoot,twoFolde), needUploadFileType)

    logging.info('the folder is:    {0}    the number of upload files:    {1}'.format(twoFolde, str(len(needUploadPathList))))
    #统计所有文件的行数
    allFileLen = sum([len(open(file,'rb').readlines()) for file in needUploadPathList])  #该目录下所有文件的行数
    logging.info('statistics rows:    {0}    the lines of all files:    {1}'.format(twoFolde, str(allFileLen)))

    for needUploadPath in needUploadPathList:
        try:
            file_handler = open(needUploadPath, 'rb')
            ftpConn.storbinary('STOR %s'%os.path.basename(needUploadPath) + tempHouZhui,file_handler,bufsize)
            file_handler.close()
            #去除临时后缀
            ftpConn.rename(os.path.basename(needUploadPath) + tempHouZhui,os.path.basename(needUploadPath))
            if int(isBackUp) == 1:  #如果ini文件中选择对文件进行备份
                try:
                    shutil.move(needUploadPath, os.path.join(os.path.join(backUpPath,today),twoFolde))
                except:
                    logging.info('file name repeat {}'.format(needUploadPath))
            else:
                #不备份的话上传结束就删除该文件
                os.remove(needUploadPath)
        except:
            traceback.print_exc()
            logging.info('{0} upload ftp filed'.format(needUploadPath))

    ftpConn.quit()


def main():
    #第一层目录下的所有第二层目录 名
    twoFoldefList = os.listdir(fileRoot)
    if int(isBackUp) == 1:
        #如果需要对数据进行备份，则在指定位置创建对应的文件夹，以天为单位
        for twoFoldef in twoFoldefList:
            if not os.path.exists(os.path.join(os.path.join(backUpPath,today),twoFoldef)):
                logging.info('need backUp file. make backUp file {0}'.format(str(os.path.join(os.path.join(backUpPath,today),twoFoldef))))
                os.makedirs(os.path.join(os.path.join(backUpPath,today),twoFoldef))

    pool = tp.ThreadPool(int(threadPoolNumber))          #多线程模块用于zip包下载和解压
    requests = makeRequests(UploadFtp,twoFoldefList)
    [pool.putRequest(req) for req in requests]
    pool.wait()


if __name__ == '__main__':
    try:
        if os.path.exists('needRemove.md'):
            exit(0)
        with open('needRemove.md','w') as f:
            pass
        configFilePath = 'config.ini'
        #cf = configparser.ConfigParser()
        cf = ConfigParser.ConfigParser()
        cf.read(configFilePath)
        #获得ftp的配置信息
        FTP_HOST = cf.get("ftpMessage","host")  #ip
        FTP_PORT = cf.get("ftpMessage","port")   #端口
        FTP_USERNAME = cf.get("ftpMessage","username")   #登录用户名
        FTP_PASSWD = cf.get("ftpMessage","passwd")      # 登录密码
        FTP_ROOT_PATH = cf.get("ftpMessage","ftpRootPath")   #存储在ftp上的根目录
        #print FTP_HOST,FTP_PORT,FTP_USERNAME,FTP_PASSWD,FTP_ROOT_PATH
        fileRoot = cf.get("fileMessage","fileRoot")   #原始文件夹的根目录
        needUploadFileType = cf.get("fileMessage","needUploadFileHouZhui")   #需要上传的文件类型
        tempHouZhui = cf.get("fileMessage","LinShiHouZhui")    #上传过程中的临时后缀
        isBackUp = cf.get("fileMessage","isBackUp")    #  是否备份
        backUpPath = cf.get("fileMessage","backUpPath")    # 备份的存储根目录
        threadPoolNumber = cf.get("fileMessage","threadPoolNumber")   #线程数
        #print(fileRoot,type(needUploadFileType),tempHouZhui,type(isBackUp),backUpPath,threadPoolNumber)
        if needUploadFileType == '':
            needUploadFileType = None    #如果想要上传目录下所有类型的文件 为None时可以上传所有类型文件
        today = time.strftime("%Y%m%d", time.localtime())
        logging.basicConfig(filename='allLog/' + 'upload_' + today + ".log", filemode='a', level=logging.NOTSET,
                            format="%(asctime)s - %(levelname)s: %(message)s")  # 日志的输出格式
        main()

    except:
        traceback.print_exc()
        logging.exception('error')
    finally:
        # 删除运行的标识符
        os.remove('needRemove.md')
        logging.info('remove needRemove.md is ok , this upload end!!!!!!!!!!!!')