# coding:utf-8
#/usr/local/bin/python3
"""
一维最大熵阈值分割法   KSW熵  一维最佳直方图熵法
"""
import numpy as np
from PIL import Image
import math

def KSW(image1):

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
    maxInormation = max(informationEntropyDict.values())
    key = [key for key in informationEntropyDict.keys() if informationEntropyDict[key] == maxInormation ]  #最佳阈值
    return  key[0]

def main():
    filename = 'img_67.jpg'
    im = Image.open(filename)
    im.load()
    im_gray = im.convert('L')  # 转换成灰度图
    key = KSW(im_gray)

    print('最佳阈值为:'+ str(key))
    image_tmp = np.asarray(im_gray)
    intensity_array = list(np.where(image_tmp<key, 0, 255).reshape(-1))   #对阈值进行切割
    im_gray.putdata(intensity_array)
    im_gray.show()
    im_gray.save('output-img_125.jpg')

if __name__ == '__main__':
    main()


