# coding:utf-8
import numpy as np

def total_pix(image):
    """
    求图像大小
    """
    size = image.shape[0] * image.shape[1]
    return size


def fast_ostu(image, threshold):
    """计算图像的直方图
    threshold： 0~255 共256个bin 按照这个分类
    统计0~i 灰度级的像素(假设像素值在此范围的像素叫做前景像素) 所占整幅图像的比例w0，并统计前景像素的平均灰度u0；
    统计i~255灰度级的像素(假设像素值在此范围的像素叫做背景像素) 所占整幅图像的比例w1，并统计背景像素的平均灰度u1；
    计算前景像素和背景像素的方差 g = w0*w1*(u0-u1) (u0-u1)
    :return  返回前景像素和背景像素的方差
    """
    image = np.transpose(np.asarray(image))   #np.asarray : 将结构数据转化为ndarray   np.transpose : 位置转置
    total = total_pix(image)                 #获得图片的大小
    bin_image = image<threshold               #落在每个bin的像素点
    sumT = np.sum(image)                     #  总像素点
    w0 = np.sum(bin_image)                    # 统计落在0 -i 的像素点
    sum0 = np.sum(bin_image * image)      #获得满足条件的数组
    w1 = total - w0                       #不满足条件的像素点
    if w1 == 0:
        return 0
    sum1 = sumT - sum0
    mean0 = sum0 / (w0 * 1.0)
    mean1 = sum1 / (w1 * 1.0)
    varBetween = w0 / (total * 1.0) * w1 / (total * 1.0) * (mean0 - mean1) * (
            mean0 - mean1)
    return varBetween