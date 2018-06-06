# coding:utf-8
"""
统计该目录下对应文件大小与行数
"""
import os
import traceback
import datetime
from threadpool import makeRequests
import threadpool as tp

express_arr = [['申通', 'data_express_shentong'],
               ['中通', 'data_express_zhongtong'],
               ['圆通', 'data_express_yuantong'],
               ['德邦物流', 'data_express_debang'],
               ['全一', 'data_express_quanyi'],
               ['百世汇通', 'data_express_bsht'],
               ['民航', 'data_express_minhang'],
               ['韵达', 'data_express_yunda'],
               ['顺丰快递', 'data_express_shunfeng'],
               ['宅急送', 'data_express_zjs'],
               ['EMS', 'data_express_ems'],
	       ['天天速递', 'data_express_tiantiankuaidi'],
	       ['华宇物流', 'data_express_huayuwuliu'],
	       ['盛辉', 'data_express_shenghui'],
	       ['盛丰', 'data_express_shengfeng'],
               ['佳吉','data_express_jiaji']
			   ]


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
    file_arr = []
    temp_arr = enum_file('%s/%s' % (scan_root, thread_info[0]), 'csv')
    #print temp_arr
    for file in temp_arr:
        if os.path.basename(file)[:8] != yestoday:
            continue
        file_arr.append(file)
        #print len(file_arr)
    sum_size = 0
    sum_line = 0
    sum_size += sum([os.path.getsize(file) for file in file_arr])
    sum_line += sum([len(open(file, 'rb').readlines()) for file in file_arr])
    total_info[thread_info[0]] = [len(file_arr), sum_size, sum_line]


if __name__ == '__main__':
    try:
        total_info = {}
        scan_root = '/data2/express_bak'
        # 当前日期
        yestoday = datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=-1), '%Y%m%d')
        # 启动线程
        pool = tp.ThreadPool(len(express_arr))
        requests = makeRequests(thread_handle, express_arr)
        [pool.putRequest(req) for req in requests]
        pool.wait()
        # 输出统计
        state_file = open('/root/output/%s_state.txt' % yestoday, 'wb')
        items = total_info.items()
        items.sort()
        for key, value in items:
            state_file.write('%s\t%d\t%d\t%d\r\n' % (key, value[2], value[0], value[1]))
        state_file.close()
    except:
        traceback.print_exc()
