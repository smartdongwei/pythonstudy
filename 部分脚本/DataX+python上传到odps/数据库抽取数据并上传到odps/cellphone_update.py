#coding:utf-8

import os
import traceback
from odps import *
import time
import re


#更新JSON
def UpdateJson(strID, strPartition):
    cFile = open('/usr/local/bin/shujumen/cellphone.json', 'rb')
    strContent = cFile.read()
    cFile.close()
    strModifyContent = re.sub('"where":"id>=\'.*\'"', '"where":"id>=\'%s\'"'%strID, strContent)
    strModifyContent = re.sub('"partition":"dt=\d*"', '"partition":"dt=%s"'%strPartition, strModifyContent)
#     print strModifyContent
    cFile = open('/usr/local/bin/shujumen/cellphone_update.json', 'wb')
    cFile.write(strModifyContent)
    cFile.close()
    
    cFile = open('/usr/local/bin/shujumen/cellphone_datahub.json', 'rb')
    strContent = cFile.read()
    cFile.close()
    strModifyContent = re.sub('"where":"id>=\'.*\'"', '"where":"id>=\'%s\'"'%strID, strContent)
    strModifyContent = re.sub('"partition":"dt=\d*"', '"partition":"dt=%s"'%strPartition, strModifyContent)
#     print strModifyContent
    cFile = open('/usr/local/bin/shujumen/cellphone_todatahub.json', 'wb')
    cFile.write(strModifyContent)
    cFile.close()



if __name__ == '__main__':
    cProcessNum = os.popen('ps aux|grep -c cellphone_update.py')
    if 3 < int(cProcessNum.read()):
        print 'Process already exists'
        exit(0)
    #获取当前时间
    strToday = time.strftime("%Y%m%d", time.localtime())
    try:
        cOdps = ODPS('ZUcC5zNLhgmBCp4J','xAJixgt2VKhvxa1LQGuRK7Qke6fEV0','kunlun','http://service.cn-xinjiang-xjkl-d01.odps.xjtz.xj/api')
        cShuRuTable = cOdps.get_table('*')
        cShuRuTable.create_partition('dt='+strToday, if_not_exists=True)
        #获取抽取标志
        print '获取抽取标志'
        with cOdps.execute_sql('select max(id) from kunlun.sjm_cellphone_daily').open_reader() as cReader:
            for cRecord in cReader:
                strID = str(int(cRecord[0])+1)
        print '当前标志:%s'%strID.encode('utf-8') 
        #更新JSON
        UpdateJson(strID, strToday)
        #exit(0)
        #启动datax
        os.system('/usr/bin/python /home/admin/datax3/bin/datax.py /usr/local/bin/shujumen/cellphone_update.json > /tmp/cellphone.log')
        os.system('/usr/bin/python /home/admin/datax3/bin/datax.py /usr/local/bin/shujumen/cellphone_todatahub.json > /tmp/cellphone_datahub.log')
        os.system('/usr/local/python-2.7/bin/python2.7 /usr/local/bin/shujumen/cellphone_datahub_import.py > /tmp/cellphone_datahub_import.log')
    except:
        traceback.print_exc()
        exit(-1)
