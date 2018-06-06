# coding:utf-8
# /user/bin/python2.7
"""
python每日练习  参考  https://github.com/Yixiaohan/show-me-the-code
将你的QQ头像（或者微博头像）右上角加上红色的数字，类似于微信未读信息数量那种提示效果。 类似于图中效果
"""
from PIL import Image, ImageDraw, ImageFont

def add_num(img):
    draw = ImageDraw.Draw(img)   # 创建一个可用来对image进行操作的对象
    myfont = ImageFont.truetype('C:/windows/fonts/Arial.ttf', size=40)  #定义字体
    fillcolor = "#ff0000"   #字体的颜色
    width, height = img.size    #获得图片的长 宽
    draw.text((width-40, 0), '99', font=myfont, fill=fillcolor)  #在图像(width-40, 0)位置处添加文字
    img.save('result.jpg','jpeg')
    return 0

if __name__ == '__main__':
    image = Image.open('image.jpg')
    add_num(image)



