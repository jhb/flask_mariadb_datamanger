from flask import Flask, current_app, g, request
from flask import request_tearing_down, got_request_exception
from flask_transaction.filedm import FileDM
from flask_transaction import init_transaction
from flask_mariadb_datamanager import init_mariadb_datamanger
import os
import tempfile

app = Flask(__name__)
app.config['TESTING'] = True
init_transaction(app)
init_mariadb_datamanger(app, host="127.0.0.1",user="joerg",password="",db="foobar",pool_size=5)
#
# @request_tearing_down.connect_via(app)
# def t(*args,**kwargs):
#     print('tearing')
#     print(args)
#     print(kwargs)
# @request_tearing_down.connect_via(app)
# def g(*args,**kwargs):
#     print('got')
#     print(args)
#     print(kwargs)


@app.route('/action')
def action():
    text1 = request.values['text1']
    text2 = request.values['text2']
    fail = request.values.get('fail',None) is not None
    basedir = tempfile.TemporaryDirectory()
    basename = os.path.normpath(basedir.name)
    print(basename)
    g.basedir = basedir
    file1 = FileDM(text1,os.path.join(basename,'foo1.txt'),current_app.tm)
    r = current_app.mariadb.cursor.execute('INSERT INTO test (id) values (5)')
    file2 = FileDM(text2, os.path.join(basename, 'bar1.txt'), current_app.tm, fail=fail)
    return 'fini'

def countid():
    with app.app_context():
        c = current_app.mariadb.cursor
        c.execute('SELECT count(id) as c from test where id=5')
        return c.fetchone()[0]

def test_good_transaction():
    basedir = None
    cid = countid()
    with app.test_client() as client:
        client.get('/action?text1=foo&text2=foo')
        basedir = g.basedir
    contents = os.listdir(os.path.normpath(basedir.name))
    assert sorted(contents) == ['bar1.txt', 'foo1.txt']
    assert countid() == cid+1

def test_good_transaction():
    basedir = None
    cid = countid()
    with app.test_client() as client:
        try:
            client.get('/action?text1=foo&text2=foo&fail=True')
        except ValueError:
            pass
        finally:
            basedir = g.basedir
    contents = os.listdir(os.path.normpath(basedir.name))
    assert sorted(contents) == []
    assert countid() == cid #nothing happened
