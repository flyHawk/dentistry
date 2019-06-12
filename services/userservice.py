import utils
import db

'''
    登录模块使用的服务
'''

current_user = {}

def auth(username, password):
    '''
        校验用户名和密码
    '''
    password = utils.md5(password)
    sql = "select count(1) from users where user_name = ? and user_password = ? "
    result = db.count(sql, (username, password))
    if result :
        current_user['username'] = username
        return True
    return False

def modify_password(old_password, new_password):
    oldpasswd = utils.md5(old_password)
    newpasswd = utils.md5(new_password)
    username =  current_user.get('username')
    
    sql = "update users set user_password= ?, update_time = ? where user_name = ? and user_password = ? "
    params = (newpasswd, utils.now() ,username, oldpasswd)
    num = db.update(sql, params)
    return True if num>0 else False

def get_current_user():
    return current_user