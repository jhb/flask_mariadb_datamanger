import os
import tempfile
from flask import Flask, g, current_app
import mariadb
from werkzeug.local import LocalProxy


class MariadbDM:

    transaction_manager = None

    def __init__(self,conn,tm):
        self.conn = conn
        self.tm = tm
        self._cursor = None
        self.join()

    def _xid(self,tx):
        return (0,str(id(tx)),str(id(self))+str(id(self.conn)))

    def join(self):
        tx = self.tm.get()
        tx.join(self)
        self.conn.tpc_begin(self._xid(tx))

    @property
    def cursor(self):
        if self._cursor is None:
            self.new_cursor()
        return self._cursor

    def new_cursor(self):
        # print('creating cursor')
        if self._cursor:
            self.close_cursor()
        self._cursor = self.conn.cursor()
        return self._cursor

    def close_cursor(self):
        if self._cursor:
            self._cursor.close()

    def abort(self,tx):
        # print('abort in adapter')
        self.conn.rollback()
        self._cursor.close()
        #self.tpc_abort(tx)

    def commit(self,tx):
        # pass
        # print('mariadb commit')
        x = 1
        pass

    def begin(self,tx):
        # print('begin mdm')
        tx.join(self)
        self.conn.tpc_begin(self._xid(tx))

    def tpc_begin(self,tx):
        if self._cursor and not self._cursor.closed:
            self.close_cursor()

    def tpc_vote(self,tx):
        self.conn.tpc_prepare()
        pass

    def tpc_finish(self,tx):
        self.conn.tpc_commit(self._xid(tx))
        pass

    def tpc_abort(self,tx):
        #print("tcp_abort")
        self.conn.tpc_rollback(self._xid(tx))

    def sortKey(self):
        return f'MariadbDataManager: {id(self)}'

    def close(self):
        #print('closing')
        self.conn.close()

    def __del__(self):
        #print('delete')
        self.close()


def get_maria_dm(name='mdm'):
    maria_pool=current_app.mariadb_pool
    dms = current_app.dms
    if name not in dms:
        try:
            c = maria_pool.get_connection()
        except mariadb.PoolError as e:
            print('getting extra connection')
            c = mariadb.connect(host="localhost", user="joerg", password="", db="foobar") #TODO
        dms[name] = MariadbDM(c,current_app.tm)
    return dms[name]

def init_mariadb_datamanger(app: Flask, **config):
    if config is None:
        config = {}

    for suffix in ['HOST','USER','PASSWORD','DB','POOL_SIZE']:
        key = f'MARIADB_{suffix}'
        config.setdefault(suffix.lower(),app.config.get(key))

    # print(config)
    mp = mariadb.ConnectionPool(**config,
                                pool_name="web-app",
                                )
    app.mariadb_pool = mp
    app.mariadb = LocalProxy(get_maria_dm)



if __name__ == '__main__':

    db = mariadb.connect(host="localhost",user="joerg",password="",db="foobar" )
    print(db)
    import glob

    for f in glob.glob('data\\*'):
        print(f)
        os.remove(f)




    import transaction
    # all not thread safe yet


    #t.join(session)

    #must be done per thread / call
    fdm1 = FileDM('heres the data', 'data\\file1.txt')
    fdm2 = FileDM('heres the data', 'data\\file3.txt')
    mdm = MariadbDM(db)


    print('action')
    result = mdm.cursor.execute("insert into test values (1)")
    result = mdm.cursor.execute("insert into test values (2)")
    transaction.manager.commit()

    print('commited')


    #
    # engine = create_engine('mysql://joerg@localhost/foobar', echo=True)
    # #conn = engine.connect()
    # #x = conn.begin_twophase()
    #
    #
    # def abort(self,tx):
    #     self.rollback()
    #
    # Session.abort = abort
    #
    # DBSession = scoped_session(sessionmaker(bind=engine,twophase=True))
    #
    #
    #
    # session = DBSession()
    # register(session,transaction_manager=transaction.manager)
    #
