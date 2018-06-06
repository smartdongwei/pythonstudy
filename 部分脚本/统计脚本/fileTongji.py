__author__ = '数据建设事业部'
#  coding:utf-8
"""
在写脚本过程中常用的方法
"""
import os
import traceback
import shutil
import platform
import datetime

COUNT_SIZE_ROWS_DICT = {}    #该数据用于存储统计信息
WORK_SYSTEM = platform.system()   #获取脚本所在系统的名称
if WORK_SYSTEM == 'windows':
    separator = '\\'
else:
    separator = '/'


def emuFiles(dirName):
    """
    获取该目录下所有文件的绝对路径，并把路径写入到 filePathList 这个列表中
    dirName：该目录的绝对路径 ,格式为字符串
    separator：如果是windwos环境下，separator为 '\\' 。如果是Linux环境下，separator为 '/'。格式为字符串
    return：返回该路径下所有的文件的路径
    """
    filePathList = []    #存储文件路径的列表
    try:
        for root, dirs, files in os.walk(dirName):
            for filespath in files:
                filePathList.append(root + separator + filespath)
        return filePathList
    except:
        traceback.print_exc()



def countSizeRows(filePathNameList,filName):
    """
    统计列表中所有文件的大小和行数，返回含有文件个数，文件大小，所有文件的总行数的字典
    :param filePathNameList: 所有文件路径列表
    :return:返回一个字典，键为写入的路径 ，值分别为该目录下的文件个数，文件大小(字节），所有文件的总行数
    """
    sum_size = 0     #文件大小
    sum_line = 0     #文件行数
    sum_size += sum([os.path.getsize(file) for file in filePathNameList])
    sum_line += sum([len(open(file, 'rb').readlines()) for file in filePathNameList])
    COUNT_SIZE_ROWS_DICT[filName] = [len(filePathNameList), sum_size, sum_line]
    return COUNT_SIZE_ROWS_DICT

def write_File(savaFilePath,fileName,dictMessage):
    """
    savaFilePath : 文件保存的路径
    fileName ：保存的文件名
    dictMessage ： 统计信息保存的字典
    """
    state_file = open(savaFilePath + separator + '%s_state.txt' % fileName, 'wb')
    items = dictMessage.items()
    items.sort()
    for key, value in items:
        try:
            state_file.write('%s\t\t%d\t%d\r\n' % (key, value[0], value[1]))
        except:
            traceback.print_exc()
    state_file.close()

def write_Oracle(dbName,dbPasswd,dbIp,dbPort,tableName,total_info):
    """
    dbName: oracle库用户名
    dbPasswd：oracle数据库密码
    dbIp ：数据库ip
    dbPort ：数据库端口
    tableName ：存储数据的表名
    total_info ：统计信息的字典
    """
    import cx_Oracle
    Today =  datetime.datetime.strftime(datetime.datetime.now() , '%Y%m%d')
    db = cx_Oracle.connect(dbName,dbPasswd, dbIp + '/' + dbPort)  #创建数据库连接
    conn = db.cursor()
    items = total_info.items()
    items.sort()
    for key, value in items:
        try:
            sql = conn.execute('INSERT INTO %s() VALUES(\'%s\', \'%s\' ,\'%s\' ,\'%s\', \'%s\')'%(tableName,key, value[2], value[0], value[1], Today))
            db.commit()
        except:
            traceback.print_exc()
    conn.close()
    db.close()


if __name__ == '__main__':

    fileRootNames=[['D:\\study1','该路径下文件的名字'],['D:\\study2','该路径下文件的名字'],
                   ['D:\\study3','该路径下文件的名字'],['D:\\study4','该路径下文件的名字']]#该需要统计文件的路径
    for fileRootName in fileRootNames:
        filePathList = emuFiles(fileRootName[0])   #调用emuFiles 函数
        countSizeRowDict = countSizeRows(filePathList,fileRootName[1])    #调用该函数
        #  把数据写入到文件中 文件名为今天的日期+_state.txt  比如 20180329_state.txt
    saveFilePath ='D:\\work' #文件保存的路径
    fileName =  datetime.datetime.strftime(datetime.datetime.now() + datetime.timedelta(days=-1), '%Y%m%d')  #该脚本运行的时间
    write_File(saveFilePath,fileName,countSizeRowDict)

    DB_NAME = 'demo'              #数据库登录名
    DB_PASSSWD = 'demo'           #数据库登录密码
    DB_IP = '**'      # 数据库ip
    DB_PORT = '****'              #数据库端口号
    TABLE_NAME = '********'    #数据库表名
    write_Oracle(DB_NAME,DB_PASSSWD,DB_IP,DB_PORT,TABLE_NAME,countSizeRowDict)

