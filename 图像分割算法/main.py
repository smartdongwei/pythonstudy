from PIL import Image
import numpy as np
from genetic import genetic

filename = 'images/yezi.jpg'

def threshold(t, image):
    print('-------------')
    print(t)
    image_tmp = np.asarray(image)
    intensity_array = list(np.where(image_tmp<t, 0, 255).reshape(-1))   #对阈值进行切割
    image.putdata(intensity_array)
    image.show()
    image.save('images/output11.jpg')

def main():
    im = Image.open(filename)
    im.load()
    im_gray = im.convert('L') # translate to  gray map
    genrtic = genetic(im_gray)
    best_threshold = genrtic.get_threshold()   #最佳切割阈值
    threshold(best_threshold, im_gray)

if __name__ == "__main__":
    main()

