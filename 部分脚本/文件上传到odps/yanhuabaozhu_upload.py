# -*- coding:utf-8 -*-
'''
主要把烟花爆竹信息插入到odps中
'''


from odps import ODPS
import time
import datetime
import os
import logging
import traceback
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

cOdps = ODPS('ZUcC5zNLhgmBCp4J', 'xAJixgt2VKhvxa1LQGuRK7Qke6fEV0', 'kunlun',
             endpoint='http://service.cn-xinjiang-xjkl-d01.odps.xjtz.xj/api')

scanFiletxt= "/data2/yanhuabaozhu"  # txt文档的存储位置
runLog="/data2/logyanhuabaozhu/"    #log存储

def enum_files(dir, ext=None):  # 获取某个文件夹中所有文件
    file_arr = []
    need_ext_filter = (ext != None)
    for root, dirs, files in os.walk(dir):
        for filespath in files:
            filepath = os.path.join(root, filespath)
            extersion = os.path.splitext(filepath)[1][1:]
            if need_ext_filter and extersion in ext:
                file_arr.append(filepath)
            elif not need_ext_filter:
                file_arr.append(filepath)
    return file_arr


def delLog(runLog):
    #删除日志
    logFileLists=[runLog + '/' + logName for logName in os.listdir(runLog)]
    for logFilePath in logFileLists:
        print logFilePath
        try:
            fileTime = time.mktime(time.localtime(os.path.getctime(logFilePath)))
            if int(openJbTime - fileTime) > 864000:
                os.remove(logFilePath)
        except:
            traceback.print_exc()


def Lib2Odps(file_arr,tableName,partition):
    try:
        logging.info('本次上传的文件路径为 '+ file_arr + tableName)
        os.system('odpscmd -e "tunnel upload %s kunlun.%s/dt=%s -dbr true -s false "' % (file_arr, tableName, partition))
        os.remove(file_arr)
    except:
        print file_path + "odps上传错误"
        logging.info(file_path + "odps上传错误")
        

def main():
    file_arrs = enum_files(scanFiletxt, 'txt')
    logging.info('本次上传的总文件数量为： '+str(len(file_arrs)))
    print file_arrs
    try:
        for file_arr in file_arrs:
            tableName = file_arr.split('/')[-1].split('-')[0]   #odps表名
            partition = file_arr.split('/')[-1].split('-')[1]   #分区名称
            table_obj = cOdps.get_table(tableName, 'kunlun')
            try:
                table_obj.create_partition('dt=' + partition, if_not_exists=True)  # 创建分区
            except:
                print '创建分区错误'

            Lib2Odps(file_arr,tableName,partition)

    except:
        logging.exception('fuck')
        print '11'

    delLog(runLog)  #删除10天前的日志文件


if __name__ == '__main__':
    today = time.strftime("%Y%m%d", time.localtime())
    logging.basicConfig(filename=runLog + today + ".log", filemode='a', level=logging.NOTSET,
                        format="%(asctime)s - %(levelname)s: %(message)s")  # 日志的输出格
    openJbTime = time.mktime(time.localtime())
    time11 = datetime.datetime.now()
    main()
    time22 = datetime.datetime.now()
    time33 = time22 - time11
    logging.info(time33)
    logging.info("--------------------------------------------------------------------")
