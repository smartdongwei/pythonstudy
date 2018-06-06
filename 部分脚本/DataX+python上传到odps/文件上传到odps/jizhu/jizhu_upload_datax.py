#coding:utf-8

import os
import traceback

import threadpool as tp
from threadpool import makeRequests

OperatorArr = [['/data1/cur/xjdx','/usr/local/bin/jizhu/json/khzl_dx.json'], ['/data1/cur/xjyd', '/usr/local/bin/jizhu/json/khzl_yd.json'],
               ['/data1/cur/xjlt','/usr/local/bin/jizhu/json/khzl_lt.json'], ['/data1/cur/xjtt', '/usr/local/bin/jizhu/json/khzl_tt.json']]


def HandleUpload(strOperator):
    print '开始上传%s'%strOperator[0][-4:]
    DateArr = os.listdir(strOperator[0])
    for strDate in DateArr:
        strPartition = strDate[:8]
        os.system('python /home/admin/datax3/bin/datax.py -p "-Ddir=%s -Dpartition=%s" %s > /data1/log/%s/%s.log'
                  % (strDate, strPartition, strOperator[1], strOperator[0][-4:], strDate))
    print '%s上传完毕'%strOperator[0][-4:]
    os.system('mv -f %s/* %/data1/bak/s'%(strOperator[0], strOperator[0][-4:]))


# 主函数入口
if __name__ == '__main__':
    #判断程序是否正在运行
    cProcessNum = os.popen('ps aux|grep -c jizhu_upload_datax.py')
    #if 3 < int(cProcessNum.read()):
        #print 'Process already exists'
        #exit(0)

    try:
        # 创建线程池
        cPool = tp.ThreadPool(len(OperatorArr))
        # 启动线程
        cRequests = makeRequests(HandleUpload, OperatorArr)
        [cPool.putRequest(req) for req in cRequests]
        cPool.wait()
    except:
        traceback.print_exc()


