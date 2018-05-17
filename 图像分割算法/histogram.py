#  -*- coding:utf-8 -*-

import numpy as np
from PIL import Image
import math
import matplotlib.pyplot as plt

def GrayscaleHistogram(image):
    AllProbabilityList = list()  # 每个灰度值像素的概率（占总像素）
    image = np.transpose(np.asarray(image))
    sumImageAll = np.sum(image)
    for threshold in range(256):
        bin_image = image == threshold
        sum0 = np.sum(bin_image * image)
        AllProbabilityList.append(sum0 / (sumImageAll * 1.0))

    plt.bar(range(len(AllProbabilityList)), AllProbabilityList)
    plt.xlabel('Grayscale')
    plt.ylabel('P(r)')
    plt.show()


def main():
    filename = 'images/img_125.jpg'
    im = Image.open(filename)
    im.load()
    im_gray = im.convert('L')  # 转换成灰度图
    GrayscaleHistogram(im_gray)

if __name__ == '__main__':
    main()