# coding:utf-8
"""
处理txt文档,对base64解码后放到照片中,回填路径 分隔符改为 ,

"""
import base64
import csv
import json
import os
from ftplib import FTP
from odps import ODPS
import traceback
import time
import shutil
import oss2
import threadpool as tp
from threadpool import makeRequests

CONST_HOST=""   #ftp帐号
CONST_PORT=""         #ftp端口号
CONST_USERNAME=""     #ftp帐号
CONST_PWD=""        #ftp密码
cOdps = ODPS()

txtFile = '/data/quanguochangkou/txtfile/'   #txt存储
csvFile = '/data/quanguochangkou/csvfile/'
ossPath = 
photoPath = '/data/quanguochangkou/photofile/'
errPath = '/data/quanguochangkou/err/'

def base64_Img(imgBase64,imgName):
    '''
    imgName  图片的名字
    :param imgBase64: 图片的base64格式
    :return
    '''
    img = base64.b64decode(imgBase64)
    imgFile = open(photoPath+'%s.jpg'%imgName,'wb')
    imgFile.write(img)
    imgFile.close()

def ftpLoad():
    ftp = FTP()
    ftp.connect(CONST_HOST, CONST_PORT)
    ftp.login(CONST_USERNAME, CONST_PWD)
    ftp.cwd('/sendfiles/changkou')
    allTxtList = ftp.nlst()
    for txtName in allTxtList:
        try:
            with open(txtFile+txtName, 'wb') as f:  # 把txt文档下载到指定的缓存目录下
                 ftp.retrbinary('RETR ' + txtName, f.write, 1024)
        except:
            traceback.print_exc()


def txt_csv(fileName):
    filePath =txtFile +fileName
    ftxt= open(filePath,'r')
    with open(csvFile+fileName.replace('.txt','.csv'),'wb') as fcsv:
        for dataLine in ftxt.readlines()[1:]:

            dataLineList = dataLine.rstrip('\r\n').split('\t')
            imgBase64 = dataLineList[4]     # 获得图片的base64格式
            imgName = dataLineList[0]        # 获得数据的唯一标志，作为图片名字
            base64_Img(imgBase64,imgName)    #生成图片
            dataLineList[4] = ossPath+imgName+'.jpg'
            all = '\t'.join(dataLineList).replace("\t",'\";\"')
            print all
            fcsv.write('\"'+all+'\"')
            fcsv.write('\r\n')

    ftxt.close()


def txt_upload_odps(csvName):
    try:
        csvPath = csvFile + csvName
        print csvPath
        #os.system('odpscmd -e "tunnel upload %s kunlun.%s/dt=%s -c "%s" -fd "%s" "' % (csvPath,tableName , partiton, 'gb2312', ';'))
        os.system('odpscmd -e "tunnel upload %s kunlun.%s/dt=%s -c \'%s\' -fd \'%s\' -rd \'%s\' -dbr true -s false"' % (csvPath, tableName, partiton, 'gb2312', ';', '\\r\\n'))
    except:
        traceback.print_exc()
        shutil.move(csvPath,errPath)


def jpg_Upload(jpgPath):
    try:
        print jpgPath
        jpgName = os.path.basename(jpgPath)
        res = cBucket.put_object_from_file('quanguochangkou' + '/' + jpgName,jpgPath)  # tableFileName +'/' + jpgName  oss项目的存储路径   jpgPath： 照片的本地路径
        shutil.move(jpgPath, errPath)
        if res.status != 200:  # 如果报错，直接把这个文件夹移到错误目录下
            print 'chucuo'+jpgPath
            shutil.move(jpgPath, errPath)
    except:
        traceback.print_exc()


def main():

    #ftpLoad()

    #allTxtNameList = [path for path in os.listdir(txtFile)]
    #for txtName in allTxtNameList:
        #print txtName
        #txt_csv(txtName)

    #for csvName in os.listdir(csvFile):
        #txt_upload_odps(csvName)

    #for jpgName in os.listdir(photoPath):
    allJpgList = [photoPath + name for name in os.listdir(photoPath)]
    try:  # 上传时的模块
        pool11 = tp.ThreadPool(10)
        requests11 = makeRequests(jpg_Upload, allJpgList)
        [pool11.putRequest(req) for req in requests11]
        pool11.wait()
    except:
        traceback.print_exc()



if __name__ == '__main__':
    tableName = ''
    partiton =  time.strftime("%Y%m%d", time.localtime())
    table_obj = cOdps.get_table(tableName, 'kunlun')  # 创建odps连接
    cEndpoint = 
    cAuth = oss2.Auth()
    cBucket = oss2.Bucket(cAuth, cEndpoint, '')
    try:
        table_obj.create_partition('dt=' + partiton, if_not_exists=True)  # 创建分区
    except:
        print '创建分区错误'

    main()
