import db

def exists(goods_name, goods_id=-1):
    sql = '''select count(1) from goods where goods_name = :goods_name and id != :goods_id'''
    return db.count(sql, (goods_name, goods_id))


def add_or_update_goods(goods_dict: dict):
    update_sql = '''update goods set goods_name =:goods_name, goods_order=:goods_order, \
        goods_unit=:goods_unit , goods_price=:goods_price, catagory_id=:catagory_id, update_time =:save_time where id =:goods_id'''
    insert_sql = '''insert into goods ( goods_name, goods_order, goods_unit, goods_price , catagory_id) \
        values( :goods_name ,:goods_order, :goods_unit, :goods_price, :catagory_id)'''
    if not  db.update(update_sql, goods_dict):
        db.insert(insert_sql, goods_dict)
    
def delete_goods(goods_id):
    sql = '''delete from goods where id = ?'''
    return db.delete(sql, (goods_id,))

def get_goods(goods_id):
    sql = '''select a.*, b.catagory_name from goods a , catagory b where a.catagory_id = b.id and a.id = ?'''
    return db.selectone(sql, (goods_id,), return_dict=True)

def get_goods_tree():
    sql = '''select a.catagory_name ,a.id, b.goods_name , b.id from catagory a left join goods b on a.ID = b.catagory_id  order by a.CATAGORY_ORDER desc, b.GOODS_ORDER desc'''
    return db.selectall(sql)
