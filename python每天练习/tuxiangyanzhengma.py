# coding:utf-8
# /user/bin/python2.7
"""
python每日练习  参考  https://github.com/Yixiaohan/show-me-the-code
使用 Python 生成类似于 图中的带字母验证码图片
"""
from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont
from pylab import *
import random,numpy,string

# string.ascii_letters :生成所有的字母 string.digits：生成所有的数字
text = random.sample((string.ascii_letters + string.digits),4)   #在这些范围内获取4个
print(text)
rawArray = numpy.zeros((100,300,3),dtype=numpy.uint8)  #产生一个100*300 的三维数组
sh = rawArray.shape    #数组的性质
for i in range(sh[0]):      #产生带模糊点的图片背景
    for j in range(sh[1]):
        for k in range(sh[2]):
            rawArray[i][j][k]=random.randint(0,255)

im = Image.fromarray(rawArray)   #图片与数组之间的转换
draw = ImageDraw.Draw(im)       #把图片转换成可操作的对象

for i in range(len(text)):
    draw.text((75*i+random.randint(0,40),random.randint(0,40)), text[i],
              font=ImageFont.truetype('C:/windows/fonts/Arial.ttf',60),
              fill = (random.randint(0,255),random.randint(0,255),random.randint(0,255)))

im.save("checkcode.jpg")