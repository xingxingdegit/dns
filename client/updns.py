#!/usr/bin/env python
# coding: utf-8
import socket, struct, fcntl, time, hashlib, logging, traceback
logging.basicConfig(filename='dydns.log',filemode='a+',level='INFO',format='%(asctime)s - %(levelname)s: %(message)s')
interval = 300  # second
server = ('www.atest.pub', 50900)

# wlan0 为网卡名，piroom为要生成的域名主机名
devs = {
        '': {'dev': 'wlan0', 'key': '', 'type': 'build'},
        '': {'dev': 'tun0', 'key': '', 'type': 'build'},         
        '': {'key': '', 'type': 'source'},
}


def get_ip(netdev):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ip = fcntl.ioctl(sock.fileno(), 0x8915, struct.pack('64s',netdev))
        sock.close()
        return socket.inet_ntoa(ip[20:24])
    except:
        logging.error(traceback.format_exc())
        return False


def encryption(data, host):
        enkey = '%s:%s' % (data, devs[host]['key'])
        sha1 = hashlib.sha1()
        sha1.update(enkey)
        return sha1.hexdigest()

while True:
    try:
        for host, value in devs.items():
            time.sleep(10)
            if value['type'] == 'build':
                ip = get_ip(value['dev'])
                if not ip:
                    continue
                data = '%s:%s' % (host, ip)
                data = '%s:%s:%s' % (data, encryption(data, host), 'build')
            elif value['type'] == 'source':
                data = '%s:%s:%s' % (host, encryption(host, host), 'source')
            else:
                logging.info('type is error')
                continue
    
            csock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            csock.connect(server)
            
            csock.send(data)
            logging.info('send: %s' % data)
            csock.close()
    except:
        logging.error(traceback.format_exc())

    time.sleep(interval)
    

