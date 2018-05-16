#!/usr/bin/env python
# coding: utf-8
import socket, subprocess, logging, pymysql, time, hashlib
import signal, traceback, os, subprocess

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(('0.0.0.0', 50900))
sock.listen(10)

directory = '/usr/local/bind/etc/zone'
rndcbin = '/usr/local/bind/sbin/rndc -c /usr/local/bind/etc/rndc.conf'

logfd = open('general.log', 'a')
errfd = open('error.log', 'a')

db = {
    'charset': 'utf8',
    'host': '',
    'user': '',
    'password': '',
    'port': 3306,
    'database': '',
}

def logger(log, type='accept'):
    time_now = time.strftime('%Y:%m:%d %H:%M:%S',time.localtime())
    if type == 'accept':
        logfd.write('%s | %s\n' % (time_now, log))
        logfd.flush()
    else:
        errfd.write('%s | %s\n' % (time_now, log))
        errfd.flush()


class handler(object):
    def __init__(self, data):
        data = data.split(':')
        if data[-1] == 'build':
            self.FQDN, self.value, self.srckey = data[:-1]
            self.enstr = ':'.join(data[0:2])
        elif data[-1] == 'source':
            self.FQDN, self.value, self.srckey = (data[0], client[0], data[1])
            self.enstr = data[0]
        else:
            logger('data is: %s, type is error' % data, 'error')
            print 'not math type'
            return False

        self.host, self.zone = self.FQDN.split('.', 1)
        self.table = '_'.join(self.zone.split('.'))
        self.file = os.path.join(directory, '%s.zone' % self.zone)
        self.conn = pymysql.connect(**db)
        self.cur = self.conn.cursor()

    def check_key(self):
        try:
            sql = 'select ukey from user where user=%r and zone=%r;' % (self.host, self.zone)
            self.cur.execute(sql)
            ukey = self.cur.fetchone()
            if not ukey:
                logger('FQDN: %s | value: %s | zone: %s | do not found the key' % (self.FQDN, self.value, self.zone), 'accept')
                print 'key is emty'
                return False
                
            ukey =  ukey[0]
            # 计算应该的key，与客户端的对比
            deskey = '%s:%s' % (self.enstr, ukey)
            sha1 = hashlib.sha1()
            sha1.update(deskey)
            deskey = sha1.hexdigest()
        
            if self.srckey == deskey:
                logger('FQDN: %s | value: %s | check successful' % (self.FQDN, self.value), 'accept')
                return True
            else:
                logger('FQDN: %s | value: %s | check failure' % (self.FQDN, self.value), 'error')
                return False
        except:
            logger(traceback.format_exc(), 'error')
            traceback.print_exc()
            return False


    def check_change(self):
        try:
            sql = 'select * from %s where type = "A" and host = %r and value = %r;' % (self.table, self.host, self.value)
            self.cur.execute(sql)
            if self.cur.fetchone():
                logger('FQDN: %s | value: %s | do not change' % (self.FQDN, self.value), 'accept')
                return False
            else:
                return True
        except:
            logger(traceback.format_exc(), 'error')
            traceback.print_exc()
            return False
    

    def updb(self):
        try:
            upsql = 'update %s set value=%r where host=%r;' % (self.table, self.value, self.host)
            insql = 'insert into %s (type, host, value) values ("A",  %r, %r);' % (self.table, self.host, self.value)
            if self.cur.execute(upsql):
                self.conn.commit()
                logger('FQDN: %s | value: %s | update db successful' % (self.FQDN, self.value), 'accept')
                return True
            elif self.cur.execute(insql):
                self.conn.commit()
                logger('FQDN: %s | value: %s | insert db successful' % (self.FQDN, self.value), 'accept')
                return True
            else:
                self.conn.rollback()
                logger('FQDN: %s | value: %s | insert or update db failure' % (self.FQDN, self.value), 'error')
                return False
        except:
            self.conn.rollback()
            logger(traceback.format_exc(), 'error')
            traceback.print_exc()
            return False

    def upfile(self):
        try:
            if os.path.exists(self.file):
                soafield = 'host,ttl,value,mail,serial,utime,atime,ltime,ctime'
                nodefield = 'type,host,ttl,value,level'
                soasql = 'select %s from soa where zone = %r' % (soafield,self.zone)
                nodesql = 'select %s from %s;' % (nodefield, self.table)

                if self.cur.execute(soasql):
                    soadata = self.cur.fetchone()
                else:
                    logger('select soa failure, function: upfile, sql: %s' % soasql, 'error')
                    return False
                if self.cur.execute(nodesql):
                    nodedata = self.cur.fetchall()
                else:
                    logger('select  failure, function: upfile, sql: %s' % nodesql, 'error')
                    return False

                # 下面写入文件
                soadict = dict(zip(soafield.split(','), soadata))
                nodelist = [dict(zip(nodefield.split(','), i)) for i in nodedata]
                
                with open(self.file, 'w') as fd:
                    soa_mail_format = '%-18s%-12s%-8s%-12s%-10s%-9s%1s\n'
                    soa_serial = '%-18s%%-12s\n' % ''
                    soa_serial_ctime = '%-18s%%-12s%%-8s\n' % ''
                    mailformat = '%-18s%-12s%-8s%-12s%-10s%-10s\n'
                    nodeformat = '%-18s%-12s%-8s%-12s%-20s\n'
                    
                    soafirst = soa_mail_format % (soadict['host'], soadict['ttl'], 'IN', 'SOA', soadict['value'], soadict['mail'], '(')
                    soaserial = soa_serial % soadict['serial']
                    soautime = soa_serial % soadict['utime']
                    soaatime = soa_serial % soadict['atime']
                    soaltime = soa_serial % soadict['ltime']
                    soactime = soa_serial_ctime % (soadict['ctime'], ')')

                    for i in (soafirst, soaserial, soautime, soaatime, soaltime, soactime):
                        fd.write(i.encode('utf-8') if isinstance(i, unicode) else i)

                    for node in nodelist:
                        if node['type'] == 'MX':
                            nodeins = mailformat % (node['host'], node['ttl'], 'IN', node['type'], node['level'], node['value'])
                        else:
                            nodeins = nodeformat % (node['host'], node['ttl'], 'IN', node['type'], node['value'])

                        fd.write(nodeins.encode('utf-8') if isinstance(nodeins, unicode) else nodeins)
                    fd.flush()
                logger('update file successful : %s' % self.file)
                return True
            else:
                logger('file not found.  file: %s' % self.file, 'error')
                
        except Exception:
            logger(traceback.format_exc(), 'error')
            traceback.print_exc()
            return False

    def reload(self):
        command = '%s reload %s' % (rndcbin, self.zone)
        comm = subprocess.Popen(command.split())
        status = comm.wait()
        if status == 0:
            logger('reload zone successful: %s' % self.zone, 'accept')
        else:
            logger('reload zone failure: %s' % self.zone, 'error')
            
while True:
    '''host:value:srckey:build'''
    '''host:srckey:source''' '''value is client ip'''
    try:
        logger('accept ......')
        csock, client = sock.accept()
        logger('client: %s is connect' % client[0])
        logger('recv data ......')
        re_data = csock.recv(1024)
        logger('recv data: %s' % re_data)

        recive = handler(re_data)
        if recive.check_key():
            if not recive.check_change():
                continue

            if recive.updb():
                if recive.upfile():
                    if recive.reload():
                        logger('reload completed')
        csock.close()

    except KeyboardInterrupt:
        csock.close()
        break
    except:
        traceback.format_exc()
        traceback.print_exc()

sock.close()
conn.close()
logfd.close()
errfd.close()
        



    
    


