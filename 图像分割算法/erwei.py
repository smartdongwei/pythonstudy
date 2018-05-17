# coding:utf-8
#/usr/local/bin/python3
"""
二维最大熵阈值分割法
"""
import numpy as np
from PIL import Image
import math
import pprint
import traceback
from collections import Counter

class Point(object):
    def __init__(self, height, width):
        self.x = height
        self.y = width

    def getX(self):
        return self.x

    def getY(self):
        return  self.y

connects = [ Point(-1, -1), Point(0, -1), Point(1, -1), Point(1, 0), Point(1, 1), Point(0, 1), Point(-1, 1), Point(-1, 0)]

def keyValueGet(entropyPoint,image,height,width):
    keyValueList = list()
    nowPixel = image[entropyPoint.x,entropyPoint.y]
    keyValueList.append(nowPixel)
    neighborhoodsList = list()    #8邻域
    for i in range(8):
        tmpX = entropyPoint.x + connects[i].x
        tmpY = entropyPoint.y + connects[i].y
        if (tmpX < 0 or tmpY < 0 or tmpX >= height or tmpY >= width):
            neighborhoodsList.append(0)
            continue
        else:
            neighborhoodsList.append(image[tmpX,tmpY])
    neighborhoodsMean = sum(neighborhoodsList)/8
    keyValueList.append(neighborhoodsMean)
    return keyValueList



def TwoDimensionalMaxEntropy(image):
    """二维最大熵阈值分割算法
    """
    allKeyValueList = list()
    image1 = np.transpose(np.asarray(image))
    height = image1.shape[0]
    width = image1.shape[1]
    img_mark = np.zeros([height, width])  # 创建等行列的数组
    for h in range(height):
        for w in range(width):
            entropyPoint = Point(h,w)
            keyValuePair = keyValueGet(entropyPoint,image1,height,width)
            img_mark[h,w] = keyValuePair[1]    # 把8邻域均值写入到对应的数组位置中
            allKeyValueList.append(keyValuePair)
    sumImageAll = height * width
    demoList = []     #  点灰度 一 区域灰度均值对 - 概率   [[121, 75, 1.7536783403188186e-05],[120, 75, 8.768391701594093e-06],[113, 70, 8.768391701594093e-06],[108, 42, 8.768391701594093e-06]]
    Pij =[]
    for keyValue in allKeyValueList:
        if keyValue not in demoList:
            demoList.append(keyValue)
            Pij.append(allKeyValueList.count(keyValue)/(sumImageAll*1.0))
    for i,demo in enumerate(demoList):
        demo.append(Pij[i])

    maxXinagSu = 0.0        #  (t,r)
    maxLinYu = 0.0
    maxInformationEntropy = 0.0    #最大信息熵
    for xiangsu in range(1,256):
        print(xiangsu)
        for linyu in range(1,256):
            PaList, PbList, HaList, HbList = [], [], [], []
            for i in demoList:
                try:
                    if i[0] <= xiangsu-1 and i[1] <= linyu-1:
                        PaList.append(i[2])
                        HaList.append(i[2]*(math.log(i[2])))
                    elif i[0] > xiangsu and i[1] > linyu:
                        PbList.append(i[2])
                        HbList.append(i[2]*(math.log(i[2])))
                except:
                    pass
            Pa, Pb = sum(PaList), sum(PbList)
            Ha, Hb = -sum(HaList), -sum(HbList)
            if Pa != 0 and Pb !=0:
                informationEntropy = math.log(Pa) + (Ha / Pa) + math.log(Pb) + (Hb / Pb)
                if informationEntropy > maxInformationEntropy:
                    maxXinagSu = xiangsu
                    maxLinYu = linyu
                    maxInformationEntropy = informationEntropy
                    print(maxInformationEntropy)
    return maxXinagSu,maxLinYu,img_mark


def main():
    filename = 'img_3941.jpg'
    im = Image.open(filename)
    im.load()
    im_gray = im.convert('L')  # 转换成灰度图
    maxXinagSu, maxLinYu,img_LinYu = TwoDimensionalMaxEntropy(im_gray)
    print(maxXinagSu, maxLinYu )
    '''
    key = TwoDimensionalMaxEntropy(im_gray)
    print('最佳阈值为:'+ str(key))

    image_tmp = np.asarray(im_gray)
    intensity_array = list(np.where(image_tmp<=key, 0, 255).reshape(-1))   #对阈值进行切割
    im_gray.putdata(intensity_array)
    im_gray.show()
    im_gray.save('output.jpg')
    '''

if __name__ == '__main__':
    main()


