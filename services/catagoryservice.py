import db


def add_or_update_catagory(catagory_dict : dict):
    update_sql = '''update catagory set catagory_name =:catagory_name, catagory_order=:catagory_order, \
        catagory_desc=:catagory_desc , update_time =:save_time where id =:catagory_id'''
    insert_sql = '''insert into catagory ( catagory_name, catagory_order, catagory_desc ) \
        values( :catagory_name ,:catagory_order, :catagory_desc)'''
    if not  db.update(update_sql, catagory_dict):
        db.insert(insert_sql, catagory_dict)

def exists(catagory_name, catagory_id=-1):
    sql = '''select count(1) from catagory where catagory_name = :catagory_name and id != :catagory_id'''
    return db.count(sql, (catagory_name, catagory_id))

def get_all_catagories():
    sql = '''select id, catagory_name, catagory_order, catagory_desc from catagory order by catagory_order desc'''
    return db.selectall(sql, params=None, return_dict=True)

def get_catagory(catagory_id):
    sql='''select id, catagory_name, catagory_order, catagory_desc from catagory where id =?'''
    return db.selectone(sql, (catagory_id,), return_dict=True)

def delete_catagory(catagory_id):
    sql = '''delete from catagory where id = ?'''
    return db.delete(sql, (catagory_id,))
    