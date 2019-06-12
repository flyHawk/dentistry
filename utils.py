import hashlib
from datetime import datetime


'''
----------------通用函数-----------------------------
'''
def md5(data):
    '''
    计算md5值
    '''
    md5_obj = hashlib.md5()
    md5_obj.update(data.encode(encoding='utf8'))
    return md5_obj.hexdigest()

def now():
    '''
    获得当前时间
    '''
    return datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')