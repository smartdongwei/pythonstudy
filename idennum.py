# coding:utf-8
# /user/bin/python2.7
#TODO（王东威）：询问谢鑫关于非正确的证件号码输出时是输出原始的信息还是输出有更改后的信息
"""
该 UDTF函数输入是两个参数   证件号码，证件类型。 输出也是两个参数     判定后的证件号码，判断标志
身份证的检测的udtf 撰写  身份证号码的构成 ： 6位地址编码 + 8位生日 + 3位顺序码 + 1位校验码
----------------------------
判断标志       含义
0              正确的身份证号
1              证件类型不是 111
2              在删除非法字符后该证件号码的长度不对
3              证件号码的长度符合，但是格式不符合正则表达式
4              证件号码长度为15，之后把证件号码转为 18位  （有一个小bug  没有对15位的身份证号码进行正则比对）
---------------------------------------
1: 判断证件类型是否为 111 是，则继续下一步操作，不是， 第一个参数返回该值，第二个参数返回     0
2：删除证件号码中的非法字符，并把最后一位的大写 X转成小写 x。 非法字符的定义:最后一位不为 x 除去最后一位其它地方存在非数字
3;判断 处理后的 证件号是否为15位或者18位  是 使用正则表达式对18位的号码进行匹配，输出对应的结果
4：对于15位的证件号码，调用函数 把15位的证件号吗转换成18位的证件号码
"""
from odps.udf import annotate
from odps.udf import BaseUDTF
import re
@annotate("string, string-> string, int")
class IdenNum(BaseUDTF):

    def __init__(self):
        self.NEW_CARD_NUMBER_LENGTH = 18   # 最新身份证号码的长度为 18位
        self.OLD_CARD_NUMBER_LENGTH = 15   # 老式身份证号码的长度为 15位
        self.ID_CARD_TYPE = '111'        # 身份证的证件类型 固定为  '111'
        #判断证件号的是否有非法的字母  非法字母的数据格式 如 qq46677777777777777 4677777777777777xx  46777df7777777777x
        self.MATCH_RULES_ONE = re.compile('^(\d+x$)|(\d+\d$)',re.I)
        #匹配身份证号码规范的正则表达式
        self.birthDateMatchRules = re.compile('^((1[1-5])|(2[1-3])|(3[1-7])|(4[1-6])|(5[0-4])|(6[1-5]))\\d{4}(?:(?!0000)' \
                                   '[0-9]{4}(?:(?:0[1-9]|1[0-2])(?:0[1-9]|1[0-9]|2[0-8])|(?:0[13-9]|1[0-2])(?:29|30)|' \
                                   '(?:0[13578]|1[02])31)|(?:[0-9]{2}(?:0[48]|[2468][048]|[13579][26])|(?:0[48]|[2468][048]|' \
                                   '[13579][26])00)0229)\\d{3}[0-9x]$',re.I)
        #用于去除身份证中非最后一位的x/X 写的正则表达式
        self.xOut =re.compile('x',re.I)
        # 15位身份证转18位身份证时的 前17个位的系数
        self.VERIFY_CODE_WEIGHT = [ 7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2]
        # 取余后的的最后一位数字的对应字典
        self.yushuDict = {0:'1',1:'0',2:'x',3:'9',4:'8',5:'7',6:'6',7:'5',8:'4',9:'3',10:'2'}

    def IDCard_15To18(self,idNumBer15):
        """
        负责把15位的身份证号码转为 18位的身份证号码
        """
        oneIdNumBer = idNumBer15[0:6] + '19' + idNumBer15[6:len(idNumBer15)]
        endSum = 0
        for key,value in enumerate(self.VERIFY_CODE_WEIGHT):
            endSum = endSum + int(oneIdNumBer[key])*value
        yushu = endSum % 11
        idNumber18 = idNumBer15 + self.yushuDict[yushu]
        return idNumber18


    def process(self,idNum,cardFlag):

        if cardFlag == self.ID_CARD_TYPE:
              # 去除证件号码中的非法字符  要注意最后一位有些正确的身份证号最后一位是字母X/x
            idNumChange = ''.join(list(filter(lambda ch:ch in '0123456789xX',idNum)))
              #  去除身份证中的非最后一位的 X/x,以及把最后一位的X转换成x
            idNumChangeEnd = self.xOut.sub('',idNumChange[:len(idNumChange)-1]) + self.xOut.sub('x',idNumChange[-1])
              #判断修改后的证件号长度是否为15 或者18
            if len(idNumChangeEnd) ==15 or len(idNumChangeEnd) ==18:
                # TODO(王东威）：编写关于15位证件号码的正则表达式，判断是否为正确的号码
                if self.birthDateMatchRules.match(idNum) is None:
                    if len(idNumChangeEnd) ==15 and idNumChangeEnd[-1] !='x':
                        #把身份证长度为15的号码  转为长度为18的身份证号码  使用 IDCard_15To18 函数转换
                        idNumChange18End = self.IDCard_15To18(idNumChangeEnd)
                        idNumEnd = idNumChange18End
                        resultFlag = 4     # 表示把15位的身份证转18位
                    else:
                        #  resultFlag =3  身份证号码不符合格式 长度不对，或者长度为15最后一位为 x
                        idNumEnd = idNumChangeEnd
                        resultFlag = 3
                else:
                    #该证件号码为正确的身份证号码，把flag设定为 0
                    idNumEnd = idNumChangeEnd
                    resultFlag = 0
            else:
                #证件号码长度不够，无法对其修改，输出原始数据，并把flag设定为2
                idNumEnd = idNumChangeEnd
                resultFlag = 2
        else:
            #判断该证件类型是否为身份证，不是标记为1，原样输出该数据
            idNumEnd = idNum
            resultFlag = 1      # 结果标记

        self.forward(idNumEnd,resultFlag)    # 输出结果



