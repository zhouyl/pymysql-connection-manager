pymysql-connection-manager
##########################

pymysql connection & pool manager for python3


Refactor pymysql connection
===========================

New features
------------

1. Parameter 'charset' default is utf8
#. Parameter 'autocommit' default is True
#. Added parameter 'timezone', default is '+00:00'
#. Use pymysql.cursors.DictCursor by default
#. Reconnect after the database connection is lost
#. Add logs for creating connections, mysql warnings, exceptions, database queries, etc.
#. Using the with...as syntax for transaction operations
#. Provide simplified query methods such as fetch_all/fetch_row/fetch_column/fetch_first
#. Provide simplified methods such as insert/insert_many/update/delete

1. Create pymysql connection
----------------------------

.. code-block:: python

  import pymysql
  from pymysql_manager import Connection

  conn = Connection(host='192.0.0.1', database='foo', timezone='+8:00')

2. Transaction
--------------

Before code:

.. code-block:: python

  try:
    conn.begin()
    conn.execute(....)
  catch Exception:
    conn.rollback()
  else:
    conn.commit()

Now:

.. code-block:: python

  with conn.transaction():
    conn.execute(...)

3. Fetch rowsets
----------------

.. code-block:: python

  # executed: select * from foo where id between 5 and 10
  all_rows = conn.fetch_all('select * from foo where id between %s and %s', 5, 10)

  # executed: select * from foo limit 1
  first_row = conn.fetch_row('select * from foo')

  # executed: select * from foo limit 1
  first_column_on_first_row = conn.fetch_first('select * from foo')

  # executed: select * from foo limit 1
  third_column_on_first_row = conn.fetch_column('select * from foo', column=3)

4. Fetch by Iterator
--------------------

When a result is large, it may be used **SSCursor**. But sometimes using **limit ... offset ...** can reduce the pressure on the database


by SSCursor

.. code-block:: python

  cursor = conn.cursor(pymysql.cursors.SSCursor)
  conn.execute(sql)
  while True:
    row = cursor.fetchone()
    if not row:
      break

by fetch_iterator

.. code-block:: python

  for row in conn.fetch_iterator(sql, per=1000, max=100000):
    print(row)

5. Single/Bulk Insert or Replace | Update | Delete
--------------------------------------------------

.. code-block:: python

  # insert ignore into mytable (foo, bar) values (1, 2)
  db.insert('insert ignore into mytable', foo=1, bar=2)

  # insert ignore into mytable (foo, bar) values (1, 2) on duplicate key update ...
  db.insert('insert ignore into mytable on duplicate key update ...', **dict(foo=1, bar=2))

  # insert ignore into mytable (id, name) values (1, 'foo'), (2, 'bar') on duplicate key update ...
  db.insert_many('insert ignore into mytable on duplicate key update ...', ['id', 'name'], [(1, 'foo'), (2, 'bar')])

  # update mytable set foo=1, bar=2 where id between %s and %s
  db.update('update mytable where id between %s and %s', 10, 5, foo=1, bar=2)
  db.update('update mytable where id between %s and %s', [10, 5], foo=1, bar=2)
  db.update('update mytable where id between %s and %s', *[10, 5], **dict(foo=1, bar=2))

  # update from mytable where id between %s and %s
  db.delete('delete from mytable id between %s and %s', 10, 5)
  db.delete('delete from mytable id between %s and %s', [10, 5])


Connection Pool
===============

1. Create connection pool
-------------------------

.. code-block:: python

  from pymysql_manager import ConnectionPooled
  pooled = ConnectionPooled(host='192.0.0.1', database='foo', 
                            pool_options=dict(max_size=10, max_usage=100000, idle=60, ttl=120))

2. Execute SQL without the connection pool
------------------------------------------

.. code-block:: python

  pooled.execute(sql)
  pooled.connection.execute(sql)

3. Using connection pool to execute SQL
---------------------------------------

.. code-block:: python

  with pooled.pool() as connection:
    connection.execute(sql)


Connection Manager
==================

1. Configuration
----------------

.. code-block:: python

  from pymysql_manager import ConnectionManager
  dm = ConnectionManager(default='foo',
                         foo=dict(host='192.0.0.1', database='foo', user='root', passwd=''),
                         bar=dict(host='192.0.0.1', database='bar', user='root', passwd=''))

2. Get a connection
-------------------

.. code-block:: python

  dm.execute(sql) # use default connection
  dm['foo].execute(sql)
  dm.connection('foo').exeucte(sql)

3. Get a connection from connection pool
----------------------------------------

.. code-block:: python

  with dm.pool() as connection: pass  # use default connection
  with dm['foo'].pool() as connection: pass
  with dm.connection('foo').pool() as connection: pass


License
=======

he MIT License (MIT). Please see License File for more information.
