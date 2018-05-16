#!/usr/bin/env python
# coding: utf-8
#
# example: python createkey.py pi.atest.pub

import base64, string, random, pymysql, sys, traceback
from pymysql import IntegrityError

user, zone = (sys.argv[1].split('.', 1))
print user, zone

db = {
    'charset': 'utf8',
    'host': '',
    'user': 'dns',
    'password': '',
    'port': 3306,
    'database': '',
}

conn = pymysql.connect(**db)
cur = conn.cursor()

def choice():
    rand_str = string.printable
    rand = ''
    for i in xrange(20):
        rand += random.choice(rand_str)
    return base64.b32encode(rand)

key = choice()
sql = 'insert into user (user, zone, ukey) values (%r, %r, %r);' % (user, zone, key)
sesql = 'select ukey from user where user = %r and zone = %r;' % (user, zone)

try:
    cur.execute(sql)
except IntegrityError:
    traceback.print_exc()
    cur.execute(sesql)
    print '%s key is exist: %s' % (user, cur.fetchone()[0])
except Exception:
    conn.rollback()
    print 'insert db fail'
    traceback.print_exc()
else:
    conn.commit()
    print 'key: %s' % key
