# -*- coding: utf-8 -*-

import time
import re
import pymysql
import logging
import warnings

from connection_pool import ConnectionPool


logger = logging.getLogger('pymysql.connection')

# 忽略 pymysql warning
warnings.filterwarnings('ignore', category=pymysql.err.Warning)

# 需要通知的查询执行时间(秒)，日志类型 INFO
NOTE_QUERY_TIME = 5

# 需要警告的查询执行时间(秒)，日志类型 WARNING
WARN_QUERY_TIME = 10

# SQL 正则模式识别符
RE_FLAG = re.I | re.S


class SQLError(Exception):
    '''SQL 语句存在错误'''


class SQLHelper(object):
    '''SQL 语句助手类'''

    @classmethod
    def inline(cls, sql):
        if isinstance(sql, (bytes, bytearray)):
            sql = sql.decode('utf8')

        return re.sub(r'\s+', ' ', sql)

    @classmethod
    def identifier(cls, ident):
        if isinstance(ident, str):
            return "`%s`" % ident

        if isinstance(ident, (tuple, list)):
            return type(ident)(map(cls.identifier, ident))

        if isinstance(ident, dict):
            return type(ident)(zip(map(cls.identifier, ident.keys()),
                                   ident.values()))

        raise ValueError("Invalid identifier value: %s" % type(ident))

    @classmethod
    def is_select(cls, sql):
        return bool(re.search(r'^\s*select\s+.*from\s+', sql, RE_FLAG))

    @classmethod
    def is_insert(cls, sql):
        return bool(re.search(r'^\s*(insert|replace)\s+.*into\s+', sql, RE_FLAG))

    @classmethod
    def is_update(cls, sql):
        return bool(re.search(r'^\s*update\s+.*set\s+', sql, RE_FLAG))

    @classmethod
    def is_delete(cls, sql):
        return bool(re.search(r'^\s*delete\s+.*from\s+', sql, RE_FLAG))

    @classmethod
    def is_write(cls, sql):
        return cls.is_insert(sql) or cls.is_update(sql) or cls.is_delete(sql)

    @classmethod
    def is_read(cls, sql):
        return not cls.is_write(sql)

    @classmethod
    def is_limited(cls, sql):
        return bool(re.search(r'\s+limit\s+\d+', sql, RE_FLAG))

    @classmethod
    def limit(cls, sql, limit):
        if cls.is_select(sql) and not cls.is_limited(sql):
            return re.sub(r'\s*$', '', sql) + ' limit %d' % limit
        return sql


class Transaction(object):
    '''支持通过 with 语法启用事务，以减少对 try/catch 的依赖，并自动 commit/rollback'''

    def __init__(self, connection):
        self._con = connection

    def __enter__(self):
        self._con.begin()
        self._con.log(logging.DEBUG, 'Transaction beginning...')
        return self._con

    def __exit__(self, exc, value, traceback):
        if exc:
            self._con.log(logging.WARN, 'Transaction rolled back: %s %s', exc, value)
            self._con.rollback()
            return False

        self._con.log(logging.DEBUG, 'Transaction committed.')
        self._con.commit()
        return True


class Connection(pymysql.connections.Connection):
    '''扩展 pymysql 连接类，提供了更多的功能扩展支持

    1. 默认编码设置为 utf8
    2. 默认开启 autocommit
    3. 支持时区指定参数 timezone，默认为 +8:00
    4. 默认使用 pymysql.cursors.DictCursor，在 fetch 时输出以列名为 key 的 dict 结果集
    5. 增加数据库丢失连接后自动重连的功能
    6. 增加数据库连接、警告、异常、查询等事件的日志记录
    7. 新增 with 语法以支持事务操作
    8. 提供 fetch_all/fetch_row/fetch_column/fetch_first 等简化查询方法
    9. 提供 insert/replace/update/delete 等简化调用方法
    '''

    # mysql 默认连接参数
    _defaults = dict(autocommit=True, charset='utf8',
                     cursorclass=pymysql.cursors.DictCursor)

    def __init__(self, timezone='+8:00', **kwargs):
        self._timezone = str(timezone)
        self._configurations = {**self._defaults, **kwargs}
        self._log_prefix = '[%s@%s] ' % (kwargs.get('host', 'localhost'),
                                         kwargs.get('database') or kwargs.get('db'))

        super().__init__(**self._configurations)

    def log(self, level, msg, *args, **kwargs):
        '''记录一条日志'''
        return logger.log(level, self._log_prefix + str(msg), *args, **kwargs)

    def connect(self, sock=None):
        '''连接到 mysql'''
        if self._sock and sock and self._sock is not sock:
            return  # 保证只连接一次

        try:
            self.log(logging.DEBUG, 'Prepare create mysql connection.')

            super().connect(sock)

            self.log(logging.DEBUG, 'Create mysql connection successfully.')

            if self._timezone:
                super().query("set time_zone='%s'" % self._timezone)
        except pymysql.err.MySQLError as e:
            errno, error = list(e.args)

            if isinstance(e, pymysql.err.Warning):
                self.log(logging.WARN, '%s: [%d] %s', type(e), errno, error)
            else:
                self.log(logging.ERROR, '%s: [%d] %s', type(e), errno, error)

            raise

    def close(self):
        '''关闭 mysql 连接'''
        super().close()
        self.log(logging.DEBUG, 'Mysql connection was closed.')

    def show_warnings(self):
        '''返回 mysql 警告消息'''
        ws = super().show_warnings()

        # 记录一下 mysql 输出的警告
        for w in ws or []:
            self.log(logging.WARN, 'MySQL %s: [%d] %s', *w)

        return ws

    def query(self, sql, unbuffered=False):
        '''执行SQL查询，当丢失连接时自动重试，并记录查询日志'''
        try:
            stime = time.time()
            rowcount = super().query(sql, unbuffered)

            self._log_query(sql, stime, rowcount)

            return rowcount
        except pymysql.err.MySQLError as e:
            self._log_query(sql, stime, ex=e)

            # 当丢失连接时，尝试重新连接并执行
            # 2006: MySQL server has gone away
            # 2013: Lost connection to MySQL server during query
            errno, error = list(e.args)
            if errno in [2006, 2013]:
                try:
                    # 如果 sock 处于 open 状态，关闭它
                    if self._sock:
                        try:
                            self._sock.close()
                            self._sock = None
                        except BaseException:
                            pass

                    self.connect()
                    self.log(logging.INFO, 'Try to reconnect successfully.')
                except pymysql.err.MySQLError as se:
                    self.log(logging.ERROR, 'Try to reconnect failed: %s', se)
                    raise

                return self.query(sql, unbuffered)

            raise

    def _log_query(self, sql, stime, rowcount=0, ex=None):
        '''记录查询日志'''
        elapsed = time.time() - stime

        # 日志级别
        if elapsed >= NOTE_QUERY_TIME:
            level = logging.WARN
        if elapsed >= WARN_QUERY_TIME:
            level = logging.WARN
        else:
            level = logging.DEBUG

        msg = 'Executed: %s, Elapsed time: %.6fs, ' % (SQLHelper.inline(sql), elapsed)

        if ex:
            errno, error = list(ex.args)
            msg += '%s: [%d] %s' % (type(ex), errno, error)
            level = logging.ERROR
        else:
            msg += 'Affected rows: %d' % rowcount

        self.log(level, msg)

    def execute(self, sql, *args):
        '''调用 cursor.execute 方法'''
        if len(args) == 1 and isinstance(args[0], (tuple, list)):
            args = args[0]

        cursor = self.cursor()
        return cursor.execute(sql, args)

    def execute_many(self, sql, args):
        '''调用 cursor.executemany 方法'''
        cursor = self.cursor()
        return cursor.executemany(sql, args)

    @property
    def rowcount(self):
        '''获取上一条 SQL 影响记录数'''
        return self.affected_rows()

    @property
    def last_insert_id(self):
        '''取出上次 insert id'''
        return self.insert_id()

    def fetch_all(self, sql, *args):
        '''取出所有结果集'''
        cursor = self.cursor()
        cursor.execute(sql, *args)
        return cursor.fetchall()

    def fetch_row(self, sql, *args):
        '''取出第一行结果集'''
        cursor = self.cursor()
        cursor.execute(SQLHelper.limit(sql, 1), *args)
        return cursor.fetchone()

    def fetch_column(self, sql, *args, column=0):
        '''取出第一行指定列结果集'''
        cursor = self.cursor(pymysql.cursors.Cursor)
        cursor.execute(SQLHelper.limit(sql, 1), *args)
        rowset = cursor.fetchone()
        return rowset[column] if rowset and column < len(rowset) else None

    def fetch_first(self, sql, *args):
        '''取出第一行第一列结果集'''
        return self.fetch_column(sql, *args, column=0)

    def fetch_iterator(self, sql, *args, max=0, per=100, callback=None):
        '''根据 SQL 迭代查询数据库

        可为大数据量的查询提供便捷的查询操作，对比 cursor 具有更好的性能

        可以有效替代以下查询代码：
            cursor.execute(sql)
            while True:
                row = cursor.fetchone()
                ...

        例如：
            for row in mysql.fetch_yields(sql, per=5000):
                print(row)
        '''
        import math
        if not SQLHelper.is_select(sql) or SQLHelper.is_limited(sql):
            raise SQLError(sql)

        offset = index = 0
        results = []

        while True:
            if max and offset >= max:
                return StopIteration

            steps = math.floor(offset / per) + 1
            if hasattr(callback, '__call__') and callback(offset, steps) is False:
                return StopIteration

            if index >= len(results):
                index = 0
                results = self.fetch_all('%s limit %d offset %d' %
                                         (sql, per, offset), *args)
                if not results:
                    return StopIteration

            offset += 1
            index += 1
            yield results[index - 1]

    def transaction(self):
        '''使用 with 语法开启事务，以减少对 try/catch 的依赖，并自动 commit/rollback

            with mysql.transaction():
                mysql.execute(...)

        相当于以下代码:

            try:
                mysql.begin()
                mysql.execute(....)
            catch Exception:
                mysql.rollback()
            else:
                mysql.commit()
        '''
        return Transaction(self)

    def insert(self, sql, *args, **data):
        '''执行 insert 操作

        db.insert('insert ignore into mytable', foo=1, bar=2)
        db.insert('insert ignore into mytable', **dict(foo=1, bar=2))
        '''
        match = re.search(r'(^\s*(insert|replace)\s+.*into\s+[`\.\w]+)(.*$)', sql, RE_FLAG)
        if data and match:
            escaped = self.escape(data)

            groups = [s.strip() for s in match.groups()]
            groups.pop(1)
            groups.insert(-1, '(%s)' % ', '.join(SQLHelper.identifier(list(escaped.keys()))))
            groups.insert(-1, 'values')
            groups.insert(-1, '(%s)' % ', '.join(escaped.values()))

            sql = ' '.join(groups)

        if not SQLHelper.is_insert(sql):
            raise SQLError(sql)

        return self.execute(sql, *args)

    def insert_many(self, sql, columns, datalist):
        '''执行批量 insert/replace 操作

        keys = ['id', 'name']
        values= [(1, 'foo'), (2, 'bar')]
        db.insert_many('insert ignore into mytable on duplicate key update...', keys, values)
        '''
        if not isinstance(columns, (list, tuple)):
            raise ValueError("<columns> must be type of list or tuple")

        if not isinstance(datalist, (list, tuple)):
            raise ValueError("<datalist> must be type of list or tuple")

        if len(columns) is not len(datalist[0]):
            raise ValueError("Coumns count doesn't match datalist values")

        match = re.search(r'(^\s*(insert|replace)\s+.*into\s+[`\.\w]+)(.*$)', sql, RE_FLAG)
        if not match:
            raise SQLError(sql)

        groups = [s.strip() for s in match.groups()]
        groups.pop(1)
        groups.insert(-1, '(%s)' % ', '.join(SQLHelper.identifier(columns)))
        groups.insert(-1, 'values')
        groups.insert(-1, ', '.join(['(%s)' % ', '.join(map(self.escape, v)) for v in datalist]))

        return self.execute(' '.join(groups))

    def update(self, sql, *args, **data):
        '''执行 update 操作

        db.update('update mytable where id < %s and id > %s', 10, 5, foo=1, bar=2)
        db.update('update mytable where id < %s and id > %s', [10, 5], foo=1, bar=2)
        db.update('update mytable where id < %s and id > %s', *[10, 5], **dict(foo=1, bar=2))
        '''
        match = re.search(r'(^\s*update\s+[`\.\w]+)\s+(.*$)', sql, RE_FLAG)
        if data and match:
            sets = ["{} = {}".format(SQLHelper.identifier(k), v)
                    for (k, v) in self.escape(data).items()]

            groups = [s.strip() for s in match.groups()]
            groups.insert(-1, 'set')
            groups.insert(-1, ', '.join(sets))
            sql = ' '.join(groups)

        if not SQLHelper.is_update(sql):
            raise SQLError(sql)

        return self.execute(sql, *args)

    def delete(self, sql, *args):
        '''执行 delete 操作
        db.delete('delete from mytable id < %s and id > %s', 10, 5)
        db.delete('delete from mytable id < %s and id > %s', [10, 5])
        '''
        if not SQLHelper.is_delete(sql):
            raise SQLError(sql)

        return self.execute(sql, *args)


class ConnectionPooled(object):
    '''数据库连接池类

    创建连接池：
        pooled = ConnectionPooled(host='192.0.0.1', database='foo',
                                  pool_options=dict(max_size=5))

    获取一个未使用连接池的连接，并执行 SQL：
        pooled.execute(sql)
        pooled.connection.execute(sql)

    使用连接池执行 SQL：
        with pooled.pool() as connection:
            connection.execute(sql)
    '''

    # 这个连接，是不进入连接池的
    _connection = None

    _defaults = dict(max_size=3, idle=30, name='mysql')

    def __init__(self, pool_options=None, **kwargs):
        self._configurations = kwargs
        pool_options = pool_options or {}
        self._pool = ConnectionPool(self._connect, **{**self._defaults, **pool_options})

    def _connect(self):
        return Connection(**self._configurations)

    @property
    def connection(self):
        if not self._connection:
            self._connection = self._connect()
        return self._connection

    def pool(self):
        return self._pool.item()

    def __getattr__(self, method):
        return getattr(self.connection, method)


class ConnectionManager(object):
    '''数据库连接管理器

    创建管理器：
        dm = ConnectionManager(default='foo',
                               foo=dict(host='192.0.0.1', database='foo'),
                               bar=dict(host='192.0.0.1', database='bar'))

    获取数据库连接：
        dm.execute(sql) # 使用默认连接
        dm['foo].execute(sql)
        dm.connection('foo').exeucte(sql)

    从连接池获取连接：
        with dm.pool() as connection: pass  # 使用默认连接
        with dm['foo'].pool() as connection: pass
        with dm.connection('foo').pool() as connection: pass
    '''

    def __init__(self, default='default', **configurations):
        self._configurations = configurations
        self._connections = {}
        self._default = default

    @property
    def default(self):
        '''获取默认连接名笱'''
        return self._default

    def connection(self, name=None):
        '''获取数据库连接'''
        if not name:
            name = self._default

        if name not in self._connections:
            self._connections[name] = self._make_connection(name)

        return self._connections[name]

    def _make_connection(self, name):
        if name not in self._configurations:
            raise ValueError('Invalid connection name [%s]' % name)

        return ConnectionPooled(**self._configurations[name])

    def __getattr__(self, method):
        '''允许直接调用默认数据库连接的方法'''
        return getattr(self.connection(), method)

    def __getitem__(self, name):
        '''获取一个数据库连接'''
        return self.connection(name)
