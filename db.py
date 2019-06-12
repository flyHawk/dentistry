import sqlite3
import logging
import os
import sys
from contextlib import contextmanager

# 加载日志模块, 开发为了方便调试，可开启debug
logger = logging.getLogger('dentistry')
#logger.setLevel(logging.DEBUG)
logger.setLevel(logging.INFO)
formater = logging.Formatter('%(asctime)-15s [%(levelname)-8s] %(module)s[%(lineno)-4d] %(name)s - %(message)s')
# 输出到文件
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(formater)
logger.addHandler(consoleHandler)

OP_INSERT = 'insert'
OP_UPDATE = 'update'
OP_DELETE = 'delete'
OP_SELECT = 'select'

_config = {}

# 字典工程，指定sqlite3返回字典形式数据
def dict_factory(cursor, row):
    d = {}
    for index, col in enumerate(cursor.description):
        d[col[0]] = row[index]
    return d

def db_config(database: str, **kw):
    global _config
    kw['database'] = str(database)
    _config = kw


def _get_connection(return_dict=False):
    logger.debug('Fetching JDBC Connection from DataSource [%s] ' % (
        _config['database'],))
    conn = sqlite3.connect(**_config)
    if return_dict:
        conn.row_factory = dict_factory
    logger.debug(
        'Obtained JDBC Connection [%s] for sqlite3 operation' % (conn,))
    return conn


def _close_connection(conn):
    logger.debug('Returning JDBC Connection [%s] to DataSource' % (conn,))
    if conn is not None:
        conn.close()



@contextmanager
def transaction(return_dict=False):
    try:
        conn = _get_connection(return_dict)
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        _close_connection(conn)


def insert(sql, params=None):
    if not sql.lower().strip().startswith(OP_INSERT):
        raise SyntaxError('Not allowed method, specifiy a insert statement.')

    with transaction() as conn:
        cur = conn.cursor()
        logger.debug('Executing Statement: %s' % sql)
        logger.debug('Parameters: %s' % str(params))
        if params:
            if isinstance(params, list):
                cur.executemany(sql, params)
            else:
                cur.execute(sql, params)
        else:
            cur.execute(sql)

        total_changes = conn.total_changes
        logger.debug('Result: %d rows inserted.' % (total_changes,))
        lastid = cur.lastrowid
        
    # 注意一次插入多行时，lastrowid返回None
    return total_changes,lastid


def update(sql, params=None):
    if not sql.lower().strip().startswith(OP_UPDATE):
        raise SyntaxError('Not allowed method, specifiy a update statement.')

    with transaction() as conn:
        cur = conn.cursor()
        logger.debug('Executing Statement: %s' % sql)
        logger.debug('Parameters: %s' % str(params))
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        total_changes = conn.total_changes
        logger.debug('Result: %d rows updated.' % (total_changes,))
    return total_changes


def delete(sql, params=None):
    if not sql.lower().strip().startswith(OP_DELETE):
        raise SyntaxError('Not allowed method, specifiy a delete statement.')

    with transaction() as conn:
        cur = conn.cursor()
        logger.debug('Executing Statement: %s' % sql)
        logger.debug('Parameters: %s' % str(params))
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        total_changes = conn.total_changes
        logger.debug('Result: %d rows deleted.' % (total_changes,))

    return total_changes


def selectone(sql, params=None, return_dict=False):
    if not sql.lower().strip().startswith(OP_SELECT):
        raise SyntaxError('Not allowed method, specifiy a select statement.')

    with transaction(return_dict) as conn:
        cur = conn.cursor()
        logger.debug('Executing Statement: %s' % sql)
        logger.debug('Parameters: %s' % str(params))
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        result = cur.fetchone()
        logger.debug('Result: %s' % (result,))

    return result


def selectall(sql, params=None,return_dict=False):
    if not sql.lower().strip().startswith(OP_SELECT):
        raise SyntaxError('Not allowed method, specifiy a select statement.')

    with transaction(return_dict) as conn:
        cur = conn.cursor()
        logger.debug('Executing Statement: %s' % sql)
        logger.debug('Parameters: %s' % str(params))
        if params:
            cur.execute(sql, params)
        else:
            cur.execute(sql)

        result = cur.fetchall()
        for row in result:
            logger.debug('Result: %s' % (row,))

    return result


def count(sql, params=None):
    row = selectone(sql, params)
    return row[0]


DB_FILE_PATH = 'dentistry.db'
INIT_DB_SCRIPTS = '''
    DROP TABLE IF EXISTS USERS;
    DROP TABLE IF EXISTS CATAGORY;
    DROP TABLE IF EXISTS GOODS;
    DROP TABLE IF EXISTS STOCKS;

CREATE TABLE USERS(
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        USER_NAME VARCHAR(32),
        USER_PASSWORD VARCHAR(64),
        CREATE_TIME DATETIME default (datetime('now', 'localtime')),
        UPDATE_TIME DATETIME
    );

CREATE TABLE [CATAGORY] (
[ID] INTEGER  PRIMARY KEY AUTOINCREMENT NULL,
[CATAGORY_NAME] VARCHAR(64)  NOT NULL,
[CATAGORY_ORDER] INTEGER DEFAULT '1' NOT NULL,
[CATAGORY_DESC] TEXT  NULL,
[CREATE_TIME] DATETIME DEFAULT 'datetime(''now'', ''localtime'')' NULL,
[UPDATE_TIME] DATETIME  NULL
);

CREATE TABLE [GOODS] (
[ID] INTEGER  PRIMARY KEY AUTOINCREMENT NULL,
[GOODS_NAME] VARCHAR(32)  NULL,
[GOODS_PRICE] DECIMAL(11, 2)  NULL,
[GOODS_UNIT] VARCHAR(12)  NULL,
[GOODS_ORDER] INTEGER DEFAULT '1' NOT NULL,
[CATAGORY_ID] INTEGER  NULL,
[CREATE_TIME] DATETIME DEFAULT 'datetime(''now'', ''localtime'')' NULL,
[UPDATE_TIME] DATETIME  NULL
);
   
CREATE TABLE STOCK (
    ID          INTEGER      PRIMARY KEY AUTOINCREMENT,
    STOCK_TYPE  TINYINT      NOT NULL,
    STOCK_DATE  DATE         NOT NULL,
    GOODS_ID    INTEGER      NOT NULL,
    GOODS_NUM   INTEGER      NOT NULL,
    GOODS_UNIT   VARCHAR (12)      NOT NULL,
    GOODS_PRICE   DECIMAL(11,2)      NOT NULL,
    OP_PERSON   VARCHAR (16),
    OP_AREA     VARCHAR (32),
    CREATE_TIME DATETIME     DEFAULT (datetime('now', 'localtime') ),
    UPDATE_TIME DATETIME,
    FOREIGN KEY (
        GOODS_ID
    )
    REFERENCES GOODS (ID) 
);
    INSERT INTO USERS (user_name, user_password) VALUES ('sysadmin','48a365b4ce1e322a55ae9017f3daf0c0');
    '''

if not os.path.exists(DB_FILE_PATH):
    # 若数据库文件不存在则初始化建库语句
    db_config(DB_FILE_PATH)
    with transaction() as conn:
        conn.executescript(INIT_DB_SCRIPTS)
else:
    db_config(DB_FILE_PATH)