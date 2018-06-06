# coding:utf-8

from threadpool import makeRequests
import traceback
import socket
import json
import threadpool as tp
from odps import ODPS
import time
import datetime
import os
import ftplib
from ftplib import FTP
import logging
import shutil

#配置参数，30种数据对应的二级目录名以及odps表名
fileOdpsNames=[['/btgnlkjbxx','ut_bt_gnlkjbxx_ss'],['/kyzbtkyzspsj','ut_bt_kyzspsj_ss'],
              ['/btswrydjxx','ut_bt_swrydjxx_ss'],['/tbChcry','ut_bfkj_wzgj_tlsp_ss'],['/bkxjWzgjMhlg','UT_bkxj_wzgj_mhlg_ss'],
              ['/gabTlsp','ut_bkxj_wzgj_tlsp_ss'],['/kkRzhy','ut_kk_rzhy_ss'],['/kkRrzhyJcz','ut_kk_rzhy_jcz_ss'],
              ['/kkRzhykyz','ut_kk_rzhy_kyz_ss'],['/kkRzhyMh','ut_kk_rzhy_mh_ss'],['/kkRzhyTl','ut_kk_rzhy_tl_ss'],
              ['/kkRzhyTlmh','ut_kk_rzhy_tlmh_ss'],['/mhzjjcgsjxx','ut_mh_mhzjjcgsjxx_ss'],
              ['/tzCjryjlBy','ut_tz_cjryjl_by_ss'],['/tlsmzxxbNew','ut_tl_tlsmzxxb_new_ss'],
              ['/tlspxx','ut_tl_tlspxx_ss'],['/wbswry','ut_wa_wbswry_ss'],['/baZazdYpYqxsTrack','UT_YP_YQXSXX_ss'],
              ['/kyzspsj','ut_yg_kyzspsj_ss'],['/gnlkjbxx','ut_za_gnlkjbxx_ss']]


root='/weiZhiShuJu'    #      ftp根目录
dowlnedRoot='/data/weiZhiShuJu/allData'         #文档下载的路经 初步定在 12.39.140.72  /data/weiZhiShuJu
logRun='/data/weiZhiShuJu/Log'        #log日志的存储位置
ERR='/data/weiZhiShuJu/err/'
CONST_HOST="12.39.3.200"   #ftp帐号
CONST_PORT="49999"         #ftp端口号
CONST_USERNAME="kexin"     #ftp帐号
CONST_PWD="5863318"        #ftp密码
cOdps = ODPS('ZUcC5zNLhgmBCp4J', 'xAJixgt2VKhvxa1LQGuRK7Qke6fEV0', 'kunlun',
             endpoint='http://service.cn-xinjiang-xjkl-d01.odps.xjtz.xj/api')


def connect():  # ftp连接代码
    try:
        ftp = FTP()
        ftp.connect(CONST_HOST, CONST_PORT)
        ftp.login(CONST_USERNAME, CONST_PWD)
        return ftp
    except:
        print "ftp连接错误"
        

def ftpDowlnedOdps(fileOdpsName):
    """
      从ftp上下载指定的txt文档,然后把txt文档上传到odps中
    """
    try:
        logging.info(fileOdpsName[0]+'开始下载和上传')
        dowlThreeDir= dowlnedRoot+fileOdpsName[0]
     
        if os.path.exists(dowlThreeDir)==False:   #判断下载路径中各三级目录是否存在，不存在则创建新目录
            os.makedirs(dowlThreeDir)

        table_obj = cOdps.get_table(fileOdpsName[1], 'kunlun')     #创建odps连接
        try: 
            table_obj.create_partition('dt=' + partition, if_not_exists=True)   # 创建分区
            print table_obj
        except:
            print '创建分区错误'
      
        ftp1=connect()                         #连接
        fileDownloads=root+fileOdpsName[0]     #txt文件所在的路径  /weizhiShuju/mhzjjcgsjxx
        ftp1.cwd(fileDownloads)                #切换目录                  
        txtListPaths=[fileDownloads+'/'+ txtFile for txtFile in ftp1.nlst()]              #获得ftp目录下的txt文档的路径 /weizhiShuju/mhzjjcgsjxx/xxxxx.txt
        logging.info(fileOdpsName[1]+'下载'+str(len(txtListPaths)))
        for txtPath in txtListPaths:
            try:  
                dowlThreeDirTxt=dowlThreeDir + '/'+txtPath.split('/')[-1]               #获得txt文档在72服务器的存储位置
                print dowlThreeDirTxt
                with open(dowlThreeDirTxt, 'wb') as f:  # 把txt文档下载到指定的缓存目录下
                    ftp1.retrbinary('RETR ' + txtPath, f.write, 1024)
                
                
                logging.info(txtPath+'下载完成')
                os.system('odpscmd -e "tunnel upload %s kunlun.%s/dt=%s -c \'%s\' -fd \'%s\' -rd \'%s\' -dbr true -s false"' % (dowlThreeDirTxt, fileOdpsName[1], partition, 'utf-8', '\\t','\\r\\n'))   #编码格式 utf-8  分隔符 ；
                logging.info(dowlThreeDirTxt+'上传完成')
                ftp1.delete(txtPath)   #删除ftp上文件
                logging.info(txtPath+'删除完成')
                os.remove(dowlThreeDirTxt)
                logging.info(dowlThreeDirTxt+'remove')   
            except:
                traceback.print_exc()
                os.move(dowlThreeDirTxt,ERR+str(fileOdpsName[1])+txtPath.split('/')[-1])
                logging.exception(dowlThreeDirTxt)
   
        ftp1.quit()
    except:
        logging.exception("上传过程中出错")
        
        
    

if __name__ == '__main__':
    
    partition = time.strftime("%Y%m%d", time.localtime())
    logging.basicConfig(filename=logRun + '/'+partition + ".log", filemode='a', level=logging.NOTSET,format="%(asctime)s - %(levelname)s: %(message)s")  # 日志的输出格式

    #fileOdpsName1=[['/gnlkjbxx','ut_za_gnlkjbxx_ss']]                 
    try:  #下载时
        pool11 = tp.ThreadPool(4)
        requests11 = makeRequests(ftpDowlnedOdps,fileOdpsNames)
        [pool11.putRequest(req) for req in requests11]
        pool11.wait()
        logging.info("所有文件上传结束")
    except:
        logging.exception("上传过程中出错")
    


