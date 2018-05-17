#  -*- coding:utf-8 -*-
"""
区域增长法  基于阈值 中心点不好选择  阈值需要确定
"""
import cv2
import numpy as np
import pprint

T = 7  # 阈值
class_k = 2  # 类别

class Point(object):
    def __init__(self, height, width):
        self.x = height
        self.y = width

    def getX(self):
        return self.x

    def getY(self):
        return  self.y

connects = [ Point(-1, -1), Point(0, -1), Point(1, -1), Point(1, 0), Point(1, 1), Point(0, 1), Point(-1, 1), Point(-1, 0)]



im = cv2.imread('img_3521.jpg')  #读入图片
im_shape = im.shape         #获取图片形状 返回行数，列数，通道数的元组
height = im_shape[0]
width = im_shape[1]
channel_Number = im_shape[2]   #通道数
img_mark = np.zeros([height , width])   #创建等行列的数组

def get_dist(seed_location1, seed_location2):
    """计算两个点的欧式距离
    :param seed_location1:  图像中的某一个元素点
    :param seed_location2:  图像中的另一个元素点r
    :return:  返回欧式距离
    """
    message_one = im[seed_location1.x,seed_location2.y]
    message_two = im[seed_location2.x, seed_location2.y]
    distance_image = np.sqrt(np.sum(np.square(message_one - message_two)))
    return  distance_image

img_re = im.copy()
for i in range(height):
    for j in range(width):
        for z in range(channel_Number):
            img_re[i, j][z] = 0


#随即取一点作为种子点
seed_list = []
seed_list.append(Point(10, 10))

while (len(seed_list) > 0):
    seed_tmp = seed_list[0]
    seed_list.pop(0)
    img_mark[seed_tmp.x, seed_tmp.y] = class_k

    #遍历8领域
    for i in range(8):
        tmpX = seed_tmp.x + connects[i].x
        tmpY = seed_tmp.y + connects[i].y
        if (tmpX < 0 or tmpY < 0 or tmpX >= height or tmpY >= width):
            continue

        distance_point = get_dist(seed_tmp, Point(tmpX, tmpY))   #计算出图像中任意点与周围邻域的距离
        # 在种子集合中满足条件的点进行生长
        if (distance_point <= T and img_mark[tmpX, tmpY] == 0):
            for z in range(channel_Number):
                img_re[tmpX, tmpY][z] = im[tmpX, tmpY][z]
            img_mark[tmpX, tmpY] = class_k
            seed_list.append(Point(tmpX, tmpY))


cv2.imshow('OUTIMAGE.jpg' , img_re)
cv2.waitKey(0)