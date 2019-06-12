import db

def add_stock(params):
    sql = '''insert into stock ( STOCK_TYPE, STOCK_DATE, GOODS_ID, GOODS_NUM, GOODS_UNIT, GOODS_PRICE, OP_PERSON,OP_AREA ) \
            values ( :STOCK_TYPE, :STOCK_DATE, :GOODS_ID, :GOODS_NUM, :GOODS_UNIT, :GOODS_PRICE, :OP_PERSON, :OP_AREA )'''
    return db.insert(sql, params)

def delete_stock(stockId):
    sql =  'delete from stock where id = ? '
    return db.delete(sql, (stockId,))

def get_stocks(conditions):
    sql = '''select a.id, a.stock_type, c.catagory_name, b.goods_name, a.stock_date, a.goods_num , a.goods_unit, a.goods_price, a.op_person, a.op_area from stock a, goods b , catagory c \
            where a.GOODS_ID = b.ID and b.CATAGORY_ID = c.ID  '''
    if conditions.get('startdate')  and conditions.get('enddate'):
        sql += ' and a.stock_date >= :startdate and a.stock_date <=:enddate'
    elif conditions.get('startdate') :
        sql += ' and a.stock_date =:startdate'
    else:
        sql += ' and a.stock_date =:enddate'
        
    if conditions.get('instock') and not conditions.get('outstock'):
        sql += ' and a.stock_type=1'
    elif not conditions.get('instock') and conditions.get('outstock'):
        sql += ' and a.stock_type=0'

    if conditions.get('goods_name') :
        sql += ' and b.goods_name like :goods_name '

    sql += ' order by a.stock_date desc, a.create_time desc'
    result = db.selectall(sql, conditions)
    return result

def get_stocks_dict(conditions):
    sql = '''select a.stock_type, c.catagory_name, b.goods_name, a.stock_date, a.goods_num , a.goods_unit, a.goods_price, a.op_person, a.op_area from stock a, goods b , catagory c \
            where a.GOODS_ID = b.ID and b.CATAGORY_ID = c.ID \
                and a.stock_date >= :startdate and a.stock_date <=:enddate'''
    if conditions.get('instock') and not conditions.get('outstock'):
        sql += ' and a.stock_type=1'
    elif not conditions.get('instock') and conditions.get('outstock'):
        sql += ' and a.stock_type=0'

    if conditions.get('goods_name') :
        sql += ' and b.goods_name like :goods_name '

    sql += ' order by c.catagory_name, b.goods_name, a.stock_type desc,  a.stock_date asc'
    result = db.selectall(sql, conditions,return_dict=True)
    return result