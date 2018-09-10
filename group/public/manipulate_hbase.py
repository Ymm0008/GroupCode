# coding=utf-8

from thrift.transport import TSocket,TTransport
from thrift.protocol import TBinaryProtocol
from hbase import Hbase

def query_hbase():
    # thrift默认端口是9090
    socket = TSocket.TSocket('192.168.0.156',9090)
    socket.setTimeout(5000)

    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    client = Hbase.Client(protocol)
    socket.open()

    print client.getTableNames()
    print client.get('t_ip_hbase','row1','cf:a')




