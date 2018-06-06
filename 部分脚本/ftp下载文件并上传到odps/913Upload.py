# coding:utf-8
from odps import ODPS
import time
import os
from ftplib import FTP

ExpressArr = [['疑似im','im_yisi'],['确定im','im_jingque'],['vpn','vpn']]
cOdps = ODPS('', '', '',endpoint='')

scan_path = '/data1/913data'
#FTP连接
def ftpconnect(host,username,password):
    ftp = FTP()
    ftp.set_debuglevel(0)
    ftp.connect(host,'21')
    ftp.login(username,password)
    ftp.getwelcome()
    return ftp

#FTP下载
def downloadfile(ftp,localpath):
    ftp_path='tzdsanhui'
    first_level = ftp.nlst(ftp_path)
    for remotepath in first_level :
	filename = os.path.basename(remotepath)
    	cfile  = open(localpath+filename,'wb')
    	ftp.retrbinary("RETR " +remotepath,cfile.write)
        # 去除文件
        ftp.delete(ftp_path+'/'+filename)
	cfile.close()
    ftp.close()

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

#循环上传到kunlun
def express_handles(file_path):
    for file_path_i in file_path:
        os.system('odpscmd -e "tunnel upload %s kunlun_dev.%s/dt=%s -c \'gbk\' -fd \' \' -dbr true -s false"' % (file_path_i, file_path_i[15:-9], partition))
        bak_path='/data1/913data_bak/'+partition
	if not os.path.exists(bak_path):
	    os.makedirs(bak_path)	
	os.system('mv %s %s' %(file_path_i,bak_path))
#处理函数
def express_handle(company_info):
    file_arr = enum_files(scan_path, 'txt')
    print(file_arr)
    express_handles(file_arr)


#修改文件后缀
def rename(files,scan_path):
    for filename in files:
        portion = os.path.splitext(filename)
        if portion[1]==".bcp":
            newname=portion[0]+".txt"
            print(newname)
            print(files)
            os.chdir(scan_path)
            os.rename(filename,newname)



#主函数
if __name__ == '__main__':
    ftp = ftpconnect("","","")
    downloadfile(ftp,"/data1/913data/")
    for i in range(3):
        data_arr = []
        table_obj = cOdps.get_table(ExpressArr[i][1], 'kunlun_dev')
        partition = time.strftime("%Y%m%d", time.localtime())
        try:
            if partition not in data_arr:
                table_obj.create_partition('dt=' + partition,if_not_exists=True)
                data_arr.append(partition)
        except:
            print ('%s创建分区错误' %ExpressArr[0][1])
    files = os.listdir(scan_path)
    rename(files,scan_path)
    express_handle(ExpressArr)

