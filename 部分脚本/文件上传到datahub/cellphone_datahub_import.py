# coding:utf-8

import sys
import traceback
from datahub import DataHub
from datahub.utils import Configer
from datahub.models import Topic, RecordType, FieldType, RecordSchema, BlobRecord, TupleRecord, CursorType
from datahub.errors import DatahubException, ObjectAlreadyExistException
import os
from threadpool import makeRequests
import threadpool as tp

# 配置添加格式['扫描路径', '扫描文件后缀', 'datahub project', 'datahub topic', '字段分隔符', '错误输出目录']
thread_infoes = [['/usr/local/bin/shujumen/cellphone_datahub', '', 'kunlun', 'sjm_cellphone_daily', '$','/tmp/datahub/cellphone_datahub_import.log']]



# 枚举文件函数
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


# 提交数据
def put_records(db, project_name, topic_name, record_list, error_path, data_file):
    # 提交数据
    failed_list = db.put_records(project_name, topic_name, record_list)
    # 判断是否有失败
    if 0 == len(failed_list): return
    # 打开输出文件
    failed_ouput = open('%s/%s.error' % (error_path, os.path.basename(data_file)), 'wb')
    # 逐行输出错误数据
    for failed_index in failed_list:
        try:
            failed_ouput.write('%s\r\n' % '\t'.join(record_list[failed_index].values))
        except:
            traceback.print_exc()
            continue
    # 关闭文件
    failed_ouput.close()


# 数据入库函数
def datahub_import(thread_info):
    try:
        # 连接datahub
        dh = DataHub('ZUcC5zNLhgmBCp4J', 'xAJixgt2VKhvxa1LQGuRK7Qke6fEV0',
                     'http://datahub.cn-xinjiang-xjkl-d01.streamcompute.xjtz.xj')
        # block等待所有shard状态ready
        dh.wait_shards_ready(thread_info[2], thread_info[3])
        # 获取topic
        topic = dh.get_topic(thread_info[3], thread_info[2])
        # 获取分区
        shards = dh.list_shards(thread_info[2], thread_info[3])

        # 写入数组记录
        records = []
        # 枚举目录下所有文件
        file_arr = enum_file(thread_info[0], thread_info[1])
        for data_file in file_arr:
            # 读取文本内容
            data_ = open(data_file, 'rb')
            content = data_.readlines()
            data_.close()
            # shard计数器
            i = 0
            # 逐行处理
            for line in content:
                try:
                    # 字符串分割
                    value_arr = line[:-2].split(thread_info[4])
                    values = []
                    for value in value_arr:
                        values.append(value)
                    # 生成record对象
                    record = TupleRecord(schema=topic.record_schema, values=values)
                    # 设置shard分区
                    record.shard_id = shards[i % len(shards)].shard_id
                    # 写入数组
                    records.append(record)
                    # 计数递增
                    i += 1
                    # 判断是否达到1W条
                    if 10000 <= i:
                        put_records(dh, thread_info[2], thread_info[3], records, thread_info[5], data_file)
                        records = []
                        i = 0
                except:
                    continue
            # 尝试写入datahub，并记录错误记录
            put_records(dh, thread_info[2], thread_info[3], records, thread_info[5], data_file)
            # TODO 文件处理完毕后续操作
        print '%s上传完毕' % thread_info[0]
    except:
        traceback.print_exc()


# 主函数
if __name__ == '__main__':
    # 设置线程数目
    thread_num = len(thread_infoes)
    # 创建线程池
    pool = tp.ThreadPool(thread_num)
    # 启动线程
    requests = makeRequests(datahub_import, thread_infoes)
    [pool.putRequest(req) for req in requests]
    # 等待线程结束
    pool.wait()
