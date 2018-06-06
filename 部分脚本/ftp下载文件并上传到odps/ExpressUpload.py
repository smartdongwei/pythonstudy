# coding:utf-8



from threadpool import makeRequests
import traceback
import socket
import os
import threadpool as tp
from odps import ODPS
import datetime
import logging
import ftplib

today = datetime.datetime.now().strftime('%Y%m%d')
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S', filename='/usr/local/bin/log/' + today + '.log', filemode='a')

ExpressArr = [
              ]

# ExpressArr = []
cOdps = ODPS()
# 扫描目录
scan_path = '/data1/express/'
# 备份目录
bak_path = '/data2/express_bak'

# 备份目录
bakinfo_path = '/data2/express_bakinfo'


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

    # 推送54所ftp 连接54所ftp
    cFtp = ftplib.FTP()
    try:
        cFtp.connect('', '')
        cFtp.login('', '')
    except:
        traceback.print_exc()

    # 遍历文件
    for file_path in file_arr:
        # partition = os.path.basename(file_path)[:8]
        try:
            # 判断是否存在数组中
            partition = os.path.basename(file_path)[:8]
            if partition not in date_arr:
                print '分区%s' % partition
                table_obj.create_partition('dt=' + partition, if_not_exists=True)
                date_arr.append(partition)

            if not os.path.exists('%s/%s/%s' % (bakinfo_path, company_info[0], partition)):
                os.makedirs('%s/%s/%s' % (bakinfo_path, company_info[0], partition))
            if not os.path.exists('%s/%s' % (bak_path, company_info[0])):
                os.makedirs('%s/%s' % (bak_path, company_info[0]))
        except:
            traceback.print_exc()
            print '%s创建分区错误' % company_info[1]
            continue

        index = file_path.rindex('/')
        fileName = file_path[index + 1:]

        # 执行上传 odps
        os.system('odpscmd -e "tunnel upload %s kunlun.%s/dt=%s -dbr true -s false"' % (
        file_path, company_info[1], partition))
        logging.info('已导入文件：' + company_info[0] + '\t' + file_path)
        # 上传54所ftp
        strNowTime = datetime.datetime.now()
        strNow = strNowTime.strftime('%Y%m%d %H:%M:%S')
        if company_info[2] != 'shengfeng':
            black_list = open(file_path, 'rb')
            try:
                cFtp.storbinary('STOR /' + company_info[2] + '/' + fileName, black_list)
                os.system('echo "已导入文件："  ' + company_info[
                    0] + '\t' + fileName + '\t' + strNow + ' >> /usr/local/bin/log_54/' + today + '.log')
            except:
                os.system('echo "导入文件失败："  ' + company_info[
                    0] + '\t' + fileName + '\t' + strNow + ' >> /usr/local/bin/log_54/' + today + '_failed.log')
                traceback.print_exc()
            black_list.close()
        # 备份文件
        os.system('cp -f %s %s/%s' % (file_path, bak_path, company_info[0]))
        os.system('mv -f %s %s/%s/%s' % (file_path, bakinfo_path, company_info[0], partition))
    try:
        cFtp.close()
    except:
        traceback.print_exc()

if __name__ == '__main__':
    # 判断程序是否正在运行
    cProcessNum = os.popen('ps aux|grep -c ExpressUpload.py')
    if 3 < int(cProcessNum.read()):
        print 'Process already exists'
        exit(0)
    #cSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #cSocket.bind(('127.0.0.1', 23421))
    #cSocket.listen(0)
    try:
        pool = tp.ThreadPool(len(ExpressArr))
        requests = makeRequests(express_handle, ExpressArr)
        [pool.putRequest(req) for req in requests]
        pool.wait()
        os.system('odpscmd -e "tunnel purge 0"')
        print '所有文件上传完毕'
    except:
        traceback.print_exc()
