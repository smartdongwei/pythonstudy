#!/bin/bash:/usr/lib64/qt-3.3/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/root/bin:/usr/lib/oracle/11.2/client/bin
# -*- coding:utf-8 -*-
"""
主要用于统计odps中的五轨数据相关表的量
"""

from odps import ODPS
import os
import logging 
import time
import datetime
import cx_Oracle
import traceback 
import threadpool as tp
from threadpool import makeRequests
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
dataTables=[['海康视频结构化数据','synods.NB_TAB_DLJKTXXX'],
['海康动态人脸数据','kunlun.hk_dong_tai_ren_lian_new_sc'],
['海康静态人脸(人脸app信息)','kunlun.ut_faceapp'],
['中电_大小热点','kunlun.ut_zd_dxrd'],
['北斗卫星wifi侦控采集数据','synods.NB_TAB_BDWXWIFIZKCJSJ'],
['ut_入户走访_刀具信息','kunlun.ut_rhzf_djxx'],
['ut_入户走访_管制器具','kunlun.ut_rhzf_gzqj'],
['ut_入户走访_户所信息','kunlun.ut_rhzf_hsxx'],
['ut_入户走访_建筑信息','kunlun.ut_rhzf_jzxx'],
['ut_入户走访_人员关系','kunlun.ut_rhzf_rygx'],
['ut_入户走访_人员信息','kunlun.ut_rhzf_ryxx'],
['ut_入户走访_图谱人员','kunlun.ut_rhzf_tpry'],
['ut_入户走访_图谱信息','kunlun.ut_rhzf_tpxx'],
['ut_一体化app_户口信息表','kunlun.YTHAPP_HKXXB'],
['ut_一体化app_人员类别映射表','kunlun.ythapp_rylbysb'],
['ut_一体化app_人员类别枚举表','kunlun.ythapp_rylbmjb'],
['ut_一体化app逾期不归境外联系人','kunlun.ythapp_yqbgjwlxr'],
['ut_一体化app_逾期不归反馈','kunlun.ythapp_yqbgfk'],
['ut_一体化app_逾期不归走访对象','kunlun.ythapp_yqbgzfdx'],
['ut_一体化app_人员信息表','kunlun.ut_personinfo'],
['ut_一体化app_可疑人员','kunlun.ythapp_kyry'],
['ut_一体化app_可疑车辆','kunlun.ythapp_kycl'],
['ut_一体化app_建筑信息表','kunlun.ut_ythapp_jzxxb'],
['ut_一体化app_寄递采集数据','kunlun.ythapp_jdcjsj'],
['ut_一体化app_车辆报备反馈','kunlun.ythapp_clbbfk'],
['ut_一体化app_人车不符反馈','kunlun.ythapp_rcbffk'],
['ut_一体化app_可疑物体','kunlun.ythapp_kywt'],
['数据门过往人员','kunlun.sjm_person_daily'],
['数据门wifi_mac','kunlun.sjm_wifi_daily'],
['数据门手机侦码','kunlun.sjm_cellphone_daily']]


def odpsTj(table_name):
    """
    用于从odps中统计出前一天分区下的五轨相关表的数据量
    """
    print table_name
    cOdps = ODPS('ZUcC5zNLhgmBCp4J', 'xAJixgt2VKhvxa1LQGuRK7Qke6fEV0',str(table_name[1].split('.')[0]),endpoint='http://service.cn-xinjiang-xjkl-d01.odps.xjtz.xj/api')
    allTj=[]
    allTj.append(table_name[0])   #把中文表名写入列表 
    if cOdps.exist_table(table_name[1].split('.')[1]) == True:   #如果表存在   
        try:
            with cOdps.execute_sql('select count(*) from %s where dt=%s'%(table_name[1],today)).open_reader() as reader:
                for record in reader: 
                    allTj.append(int(record[0]))
        except:
            allTj.append(0)

        try:
            with cOdps.execute_sql('select count(*) from %s where dt=%s'%(table_name[1],yesterdayPartiton)).open_reader() as reader:
                for record in reader: 
                    allTj.append(int(record[0]))
        except:
            allTj.append(0)

        try:
            with cOdps.execute_sql('select count(*) from %s '%(table_name[1])).open_reader() as reader:
                for record in reader: 
                    allTj.append(int(record[0]))
        except:
            allTj.append(0)
    else:
        for i in range(3):
            allTj.append(0)

    print allTj
    insertDataOracle(allTj)

        


def insertDataOracle(allTj):
    dalei='地面感知数据'
    cDataBase=cx_Oracle.connect('monitoring','monitoring','12.39.141.163:1521/orcl')  #进行数据库连接
    cCursor=cDataBase.cursor()   #创建游标

    try:
        cCursor.execute('delete monitoring.dp_statis_DiMianGanZhiTJ_odps where dalei = \'%s\' and xiaolei = \'%s\'' % (dalei,allTj[0]))
        #print 'delete monitoring.dp_statis_DiMianGanZhiTJ_odps where dalei = \"%s\" and xiaolei = \"%s\"' % (dalei,allTj[0])
        #cDataBase.commit()
        insert_sql = 'insert into monitoring.dp_statis_DiMianGanZhiTJ_odps(dalei,xiaolei,jrtj,zrtj,zl) values(\'%s\',\'%s\',%d,%d,%d)' % (dalei,allTj[0],allTj[1],allTj[2],allTj[3])
        #print insert_sql
        cCursor.execute(insert_sql)
        cDataBase.commit()   #关闭游标
        cCursor.close()
        cDataBase.close()  #关闭数据库连接
        
    except:
        cDataBase.rollback()
        cDataBase.commit()
        cCursor.close()
        cDataBase.close()
        traceback.print_exc()
    
    
    


if __name__ == '__main__':
    
    #odps 的连接配置
    #f=open('/data1/1.txt','w')
    today=time.strftime('%Y%m%d',time.localtime())    #当天日期
    yesterdayPartiton=(datetime.datetime.now()+datetime.timedelta(days=-1)).strftime('%Y%m%d')
    dataTable=['海康视频结构化','kunlun.ut_kk_rzhy_jcz']
    try:  # 下载时的多线程
        pool = tp.ThreadPool(3)
        requests = makeRequests(odpsTj,dataTables)
        [pool.putRequest(req) for req in requests]
        pool.wait()
    except:
        print  "下载过程中出错"
    #f.write(today)
    #f.close()
    #odpsTj(dataTable)
    
