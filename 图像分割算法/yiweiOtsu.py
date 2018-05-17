# coding:utf-8
#/usr/local/bin/python3
"""
基于一维最大熵的Otsu阈值分割法
"""
import numpy as np
from PIL import Image
import math

def KSWOtsu(image1):

    AllProbabilityDict = dict()   # 每个灰度值像素的概率（占总像素）
    image = np.transpose(np.asarray(image1))
    sumImageAll = np.sum(image)
    for threshold in range(256):
        bin_image = image  == threshold
        sum0 = np.sum(bin_image * image)
        AllProbabilityDict[threshold] = sum0 / (sumImageAll*1.0)

    informationEntropyDict = dict()   #信息熵的字典总数

    for threshold in range(256):
        W1 = sum([AllProbabilityDict.get(i) for i in range(threshold )])*1.0       #获得背景的灰度分布概率
        W2 = sum([AllProbabilityDict.get(i) for i in range(threshold,256)])*1.0    #运动目标的灰度分布概率
        if W1 != 0:                               #求背景的信息熵
            H1 = -sum([math.log(AllProbabilityDict.get(i)/W1)*(AllProbabilityDict.get(i)/W1) for i in
                        range(threshold) if AllProbabilityDict.get(i)!=0])
        else:
            H1 = 0.0

        if W2 != 0:                          #求运动目标的信息熵
            H2 = -sum([math.log(AllProbabilityDict.get(i) / W1) * (AllProbabilityDict.get(i) / W1) for i in
                        range(threshold, 256) if AllProbabilityDict.get(i) != 0])
        else:
            H2 = 0.0
        informationEntropyDict[threshold] = H2+H1
    maxFangcha = 0
    maxHx = 0.0

    for Hx in list(informationEntropyDict.values()):
        print(Hx)
        Hi, HiOut = [], []
        HiProbability,HiOutProbability = [], []
        for i in range(256):
            if informationEntropyDict[i] < Hx :
                Hi.append(informationEntropyDict[i])
                HiProbability.append(AllProbabilityDict[i])
            else:
                HiOut.append(informationEntropyDict[i])
                HiOutProbability.append(AllProbabilityDict[i])

        HiMean = sum(Hi)/len(Hi) if len(Hi)>0 else 0
        HiOutMean = sum(HiOut)/len(HiOut) if len(HiOut)>0 else 0
        Wa = sum(HiProbability)
        Wb = sum(HiOutProbability)
        try:
            fangcha = Wa*Wb*(HiMean - HiOutMean)*(HiMean - HiOutMean)
            if fangcha > maxFangcha:
                maxFangcha = fangcha
                maxHx = Hx
                print(maxFangcha)
        except:
            pass

    key = [key for key in informationEntropyDict.keys() if informationEntropyDict[key] == maxHx]  # 最佳阈值
    return key[0]


    

def main():
    filename = 'img_125.jpg'
    im = Image.open(filename)
    im.load()
    im_gray = im.convert('L')  # 转换成灰度图
    #key = KSWOtsu(im_gray)
    key =16
    print('最佳阈值为:'+ str(key))
    image_tmp = np.asarray(im_gray)
    intensity_array = list(np.where(image_tmp>key, 0, 255).reshape(-1))   #对阈值进行切割
    im_gray.putdata(intensity_array)
    im_gray.show()
    im_gray.save('output-erwei.jpg')


if __name__ == '__main__':
    main()


