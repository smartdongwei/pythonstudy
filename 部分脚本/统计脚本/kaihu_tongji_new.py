﻿# coding:utf-8

import os
import traceback
import datetime
from threadpool import makeRequests
import threadpool as tp
import cx_Oracle

# 扫描目录
kh_scan_path = [['移动', '/data1/cur/xjyd'], ['联通', '/data1/cur/xjlt'], ['电信', '/data1/cur/xjdx'], ['铁通', '/data1/cur/xjtt']]

# 枚举文件
def enum_file(dir, ext=None):
    Files = []
    NeedExtFilter = (ext != None)
    for root, dirs, files in os.walk(dir):
        for filespath in files:
            filepath = os.path.join(root, filespath)
            extersion = os.path.splitext(filepath)[1][1:]
            if NeedExtFilter and extersion in ext:
                Files.append(filepath)
            elif not NeedExtFilter:
                Files.append(filepath)
    return Files

# 线程统计函数
def thread_handle(thread_info):
    sum_size = 0
    sum_line = 0
    sum_size += sum([os.path.getsize(file) for file in thread_info[1]])
    sum_line += sum([len(open(file, 'rb').readlines()) for file in thread_info[1]])
    total_info[thread_info[0]] = [len(thread_info[1]), sum_size, sum_line]

if __name__ == '__main__':
    try:
        total_info = {}
        file_arr = []
        # 线程启动数组
        thread_arr = []
        total_file = {}

        # 枚举文件
        for scan_path in kh_scan_path:
            temp_file_arr = enum_file(scan_path[1])
            thread_arr.append([scan_path[0], temp_file_arr])

        # 当前日期
        yestoday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=-1), '%Y%m%d')
        Today = datetime.datetime.now().strftime('%Y%m%d')
        # 启动线程
        pool = tp.ThreadPool(len(thread_arr))
        requests = makeRequests(thread_handle, thread_arr)
        [pool.putRequest(req) for req in requests]
        pool.wait()
        # 输出统计
        state_file = open('/root/output/%s_state.txt' % yestoday, 'wb')
        db_conn = cx_Oracle.connect('monitoring/monitoring@12.39.141.163/orcl')
        conn = db_conn.cursor()
        items = total_info.items()
        items.sort()
        for key, value in items:
            state_file.write('%s\t%d\t%d\t%d\r\n' % (key, value[2], value[0], value[1]))
            sql = conn.execute('INSERT INTO JKPT_TJ_RECEIVE (DATA_NAME, RECEIVE_DATE, FILE_ROWS, FILE_COUNT, FILE_SIZE, UPLOAD_DATE) VALUES(\'%s\', \'%s\', \'%s\' ,\'%s\' ,\'%s\', \'%s\')'%(key, yestoday, value[2], value[0], value[1], Today))
            print 'inserted'
            db_conn.commit()
        conn.close()
        db_conn.close()

        state_file.close()
    except:
        traceback.print_exc()
