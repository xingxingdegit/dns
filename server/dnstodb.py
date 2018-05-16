#!/usr/bin/env python
# coding: utf-8
#

import pymysql, sys, traceback, os

db = {
    'charset': 'utf8',
    'host': '',
    'user': '',
    'password': '',
    'port': 3306,
    'database': '',
}

fd = open(sys.argv[1])

filesize = os.path.getsize(sys.argv[1])
zone_name = '.'.join(sys.argv[1].split('.')[:-1])
table_name = '_'.join(zone_name.split('.'))

conn = pymysql.connect(**db)
cur = conn.cursor()

def insert_db(table, data):
    vv = data.values()
    value = len(vv) * '%r,' % tuple(vv)
    sql = 'insert into %s (%s) values (%s);' % (table, ','.join(data.keys()), value.rstrip(','))
    print sql
    return cur.execute(sql)

def jump_line():
    while True:
        if fd.tell() >= filesize:
            print 'file read completed'
            return True
        data_line = fd.readline().strip()
        if not data_line:
            continue
        elif list(data_line.split()[0])[0] == '#':
            continue
        else:
            return data_line.strip()
      

def get_soa():
    soa_list = jump_line().split()
    if soa_list[-1].strip() == '(':
        if len(soa_list) == 7:
            host, ttl, _, type, value, mail, _ = soa_list
        else:
            print 'soa pary error'
            return False

        serial = jump_line().split()[0]
        utime = jump_line().split()[0]
        atime = jump_line().split()[0]
        ltime = jump_line().split()[0]
        line_list = jump_line().split()
        print line_list
        if ')' not in line_list:
            print 'soa ) is error'
            return False
        ctime = line_list[0]
        
    elif soa_list[-1].strip() == ')':
        print 'file soa format too low'
        return False

    data = {'zone': zone_name, 'host': host, 'ttl': int(ttl), 'value': value,
            'mail': mail, 'serial': int(serial), 'utime': utime, 'atime': atime,
            'ltime': ltime, 'ctime': ctime}
    return data

def get_node(node_line):
    node_line = node_line.split()
    if len(node_line) == 5:
        host, ttl, _, type, value = node_line
        data = {'type': type, 'host': host, 'ttl': ttl, 'value': value}
    elif len(node_line) == 6 and node_line[3] == 'MX':
        host, ttl, _, type, level, value = node_line
        data = {'type': type, 'host': host, 'ttl': ttl, 'value': value, 'level': level}
    else:
        print 'node party error'
        return False

    return data

def main():
    try:
        soa_data = get_soa()
        if not soa_data:
            print '2'
            exit()
        
        status = insert_db('soa', soa_data)
        print 'status is %s' % status
        if not status:
            raise 'soa insert error'

        while True:
            data_line = jump_line()
            if data_line == True:
                break
            elif data_line:
                node_data = get_node(data_line)
                if node_data:
                    status = insert_db(table_name, node_data)
                    if status:
                        continue
            else:
                raise 'node insert error'
    except Exception:
        traceback.print_exc()
        conn.rollback()
    else:
        conn.commit()


main()




    
    
    
        
            
    


