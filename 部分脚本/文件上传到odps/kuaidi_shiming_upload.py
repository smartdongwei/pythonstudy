# coding:utf-8

from threadpool import makeRequests
import traceback
import socket
import os
import threadpool as tp
from odps import ODPS
import time
import logging

ExpressArr = [['快递实名数据', '']
              ]
              
cOdps = ODPS()
# 扫描目录
scan_path = '/data/txt_express/'
logFile='/root/kuaiDiLog/'      #存放log日志的位置

# 遍历文件夹
def enum_files(dir, ext=None):
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


# 快递公司分发函数
def express_handle(company_info):
    file_arr = enum_files(scan_path, 'txt')
    print '%s处理文件数%d' % (company_info[0], len(file_arr))
    # 遍历文件
    logging.info(company_info[1])
    logging.info(len(file_arr))
    try:
        pool11 = tp.ThreadPool(10)
        requests11 = makeRequests(express_handles, file_arr)
        [pool11.putRequest(req) for req in requests11]
        pool11.wait()
        os.system('odpscmd -e "tunnel purge 0"')
        logging.info("所有文件上传完毕")
    except:
        logging.info("文件上传出错")


def express_handles(file_path):
    logging.info(file_path)
    try:
        # 执行上传
        os.system('odpscmd -e "tunnel upload %s kunlun.%s/dt=%s -c "gbk" -fd "\|++\|" -dbr true -s false"' % (file_path, ExpressArr[0][1], partition))
        # 去除文件
        os.system('rm -f %s' % (file_path))
    except:
        logging.info("文件上传出错")


if __name__ == '__main__' :
    # 判断程序是否正在运行

    date_arr = []
    table_obj = cOdps.get_table(ExpressArr[0][1], 'kunlun')
    partition = time.strftime("%Y%m%d", time.localtime())
    try:
        # 判断是否存在数组中
        if partition not in date_arr:
            print '分区%s' % partition
            table_obj.create_partition('dt=' + partition, if_not_exists=True)
            date_arr.append(partition)
    except:
        print '%s创建分区错误' % ExpressArr[0][1]
        logging.info("创建分区错误")


    cProcessNum = os.popen('ps aux|grep -c kuaidi_shiming_upload.py')
    if 3 < int(cProcessNum.read()):
        print 'Process already exists'
        exit(0)

    logging.basicConfig(filename=logFile + partition + ".log", filemode='a', level=logging.NOTSET,
                        format="%(asctime)s - %(levelname)s: %(message)s")  # 日志的输出格式
    try:
        pool = tp.ThreadPool(len(ExpressArr))
        requests = makeRequests(express_handle, ExpressArr)
        [pool.putRequest(req) for req in requests]
        pool.wait()
        os.system('odpscmd -e "tunnel purge 0"')
        logging.info("所有文件上传完毕")
    except:
        logging.info("文件上传出错")
