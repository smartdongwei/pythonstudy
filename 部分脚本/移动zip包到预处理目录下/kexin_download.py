#coding:utf-8


import os
import traceback


#遍历文件夹
def EnumFiles(dir, ext=None):
    Files = []
    NeedExtFilter = (ext != None)
    for root,dirs,files in os.walk(dir):
        for filespath in files:
            filepath = os.path.join(root,filespath)
            extersion = os.path.splitext(filepath)[1][1:]
            if NeedExtFilter and extersion in ext:
                Files.append(filepath)
            elif not NeedExtFilter:
                Files.append(filepath)
    return Files


#主函数
if __name__=='__main__':
    #输出目录
    strToPath = "/data1/input"
    try:
        FileArr = EnumFiles("/mnt/kexin1", "zip")
        for File in FileArr:
            print "移动文件%s"%File
            os.system("mv %s %s/%s.ing"%(File, strToPath, os.path.basename(File)))
            os.system("mv %s/%s.ing %s/%s"%(strToPath, os.path.basename(File), strToPath, os.path.basename(File)))
    except:
        traceback.print_exc()

