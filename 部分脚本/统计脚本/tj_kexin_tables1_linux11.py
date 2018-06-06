#!/usr/local/python-2.7/bin/python
#coding:utf-8

import cx_Oracle
import os
from threadpool import makeRequests
from datetime import timedelta
import threadpool as tp
import datetime
import logging
import traceback
import sys

reload(sys)
# 使系统默认编码与库一致，防止乱码
sys.setdefaultencoding('utf-8')

logfilename = datetime.datetime.now().strftime('%Y%m%d%H')
#设置日志
logging.basicConfig(level=logging.INFO,format='%(message)s',filename='/root/log/' + '大屏数据接入量统计_' + logfilename + '.txt',filemode='w')
#设置环境变量
#os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.ZHS16GBK'
os.environ['NLS_LANG']='SIMPLIFIED CHINESE_CHINA.UTF8'
#需要统计的表:表名,数据名
tables = [['UT_治安_娱乐业附件表','娱乐业附件表'],
['UT_治安_娱乐业从业人员基本信息','治安_娱乐业从业人员基本信息'],
['UT_治安_用章单位','治安_用章单位'],
['UT_治安_印章制作单位信息','治安_印章制作单位信息'],
['UT_治安_印章基本信息','印章基本信息'],
['UT_治安_疑似漂白身份人员','治安_疑似漂白身份人员'],
['UT_治安_烟花爆竹人员基本信息','治安_烟花爆竹人员基本信息'],
['UT_治安_烟花爆竹单位基本信息','治安_烟花爆竹单位基本信息'],
['UT_治安_物品入库发','UT_治安_物品入库发']]

today = datetime.datetime.now().strftime('%Y%m%d')
yestoday = (datetime.datetime.now() + timedelta(days=-1)).strftime('%Y%m%d')

# 统计结果类
class count(object):
    def __init__(self,total):
        self.total = total

class resultinfo(object):
    def __init__(self):
        self.dalei = None
        self.xiaolei = None
        self.jrtj = 0
        self.zrtj = 0
        self.zl = 0

# 统计数据
def statistics_table(table):
    #print table.decode('utf-8')
    resinfo = resultinfo()
    resinfo.dalei='社会数据'
    resinfo.xiaolei = table[1]
    db = cx_Oracle.connect('data_user', 'sh_user', '176.0.0.234:1521/bissdb')
    cursor = db.cursor()
    # 统计今日入库量
    print '统计：' + table[0]
    sql = 'select count(*) as total from bissut.' +table[0]+ ' where to_char(etltime,\'yyyymmdd\') = \'' + today + '\''
    cursor.execute(sql)
    cursor.rowfactory = count
    info = ''
    for row in cursor:
        info = '社会数据\t' + table[0] + '\t%d' % row.total
        resinfo.jrtj = row.total
    # 统计昨日入库量
    sql = 'select count(*) as total from bissut.' + table[0] + ' where to_char(etltime,\'yyyymmdd\') = \'' + yestoday + '\''
    cursor.execute(sql)
    cursor.rowfactory = count
    for row in cursor:
        info = info + '\t%d' % row.total
        resinfo.zrtj = row.total
    # 统计数据总量
    sql = 'select count(*) as total from bissut.' + table[0]
    cursor.execute(sql)
    cursor.rowfactory = count
    for row in cursor:
        info = info + '\t%d' % row.total
        resinfo.zl = row.total
    print info
    print '统计完成：' + table[0].decode('utf-8') + '。'
    logging.info(info)
    print '写入数据库。。。'.decode('utf-8')
    write2db(resinfo)
    db.close()

# 将统计结果插入数据库
def write2db(resinfo):
    print 'dalei:%s\txiaolei:%s\tjrtj:%d\tzrtj:%d\tzl:%d' % (resinfo.dalei,resinfo.xiaolei,resinfo.jrtj,resinfo.zrtj,resinfo.zl)
    db = cx_Oracle.connect('monitoring','monitoring','12.39.141.163:1521/orcl')
    try:
        cursor = db.cursor()
        cursor.execute('delete monitoring.dp_statis_old234 where dalei = \'%s\' and xiaolei = \'%s\'' % (resinfo.dalei,resinfo.xiaolei))
        db.commit()
        insert_sql = 'insert into monitoring.dp_statis_old234(dalei,xiaolei,jrtj,zrtj,zl) values(\'%s\',\'%s\',%d,%d,%d)' % (resinfo.dalei,resinfo.xiaolei,resinfo.jrtj,resinfo.zrtj,resinfo.zl)
        print insert_sql
        cursor.execute(insert_sql)
        db.commit()
        db.close()
        print '写入完成：' + resinfo.xiaolei.decode('utf-8')
    except:
        db.rollback()
        db.close()
        print '写入失败：' + resinfo.xiaolei.decode('utf-8')
        traceback.print_exc()

if __name__ == '__main__':
    cPool = tp.ThreadPool(8)
    # 启动线程
    cRequests = makeRequests(statistics_table, tables)
    [cPool.putRequest(req) for req in cRequests]
    cPool.wait()

