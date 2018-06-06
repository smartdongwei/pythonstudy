# coding:utf-8



from threadpool import makeRequests
import traceback
import socket
import os
import threadpool as tp
from odps import ODPS

ExpressArr = [['民航', 'data_express_minhang']]
# ExpressArr = ['宅急送', '顺丰', '全一', '民航', '德邦', 'EMS']
cOdps = ODPS('ZUcC5zNLhgmBCp4J', 'xAJixgt2VKhvxa1LQGuRK7Qke6fEV0', 'kunlun',
             endpoint='http://service.cn-xinjiang-xjkl-d01.odps.xjtz.xj/api')
# 扫描目录
scan_path = '/data1/express/'
# 备份目录
bak_path = '/data2/express_bak'


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
    file_arr = enum_files(scan_path + '%s' % company_info[0], 'csv')
    print '%s处理文件数%d' % (company_info[0], len(file_arr))
    date_arr = []
    table_obj = cOdps.get_table(company_info[1], 'kunlun')
    # 遍历文件
    for file_path in file_arr:
        try:
            # 判断是否存在数组中
            partition = os.path.basename(file_path)[:8]
            if partition not in date_arr:
                print '分区%s' % partition
                table_obj.create_partition('dt=' + partition, if_not_exists=True)
                date_arr.append(partition)
        except:
            traceback.print_exc()
            print '%s创建分区错误' % company_info[1]
            continue
        # 执行上传
        os.system('odpscmd -e "tunnel upload %s kunlun.%s/dt=%s -dbr true -s false"' % (file_path, company_info[1], partition))
        # 备份文件
        os.system('mv -f %s %s/%s' % (file_path, bak_path, company_info[0]))


if __name__ == '__main__':
    # 判断程序是否正在运行
    #cProcessNum = os.popen('ps aux|grep -c ExpressUpload.py')
    #if 3 < int(cProcessNum.read()):
    #    print 'Process already exists'
    #    exit(0)
    cSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    cSocket.bind(('127.0.0.1', 9421))
    cSocket.listen(0)
    try:
        pool = tp.ThreadPool(len(ExpressArr))
        requests = makeRequests(express_handle, ExpressArr)
        [pool.putRequest(req) for req in requests]
        pool.wait()
        os.system('odpscmd -e "tunnel purge 0"')
        print '所有文件上传完毕'
    except:
        traceback.print_exc()
