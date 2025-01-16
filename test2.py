from flask import Flask,jsonify
from Scripts.OPAPI_36 import *
import time
import _thread
from datetime import datetime, timezone, timedelta







def executeQuery():
    'SQL(增、删、改、查)'
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD
    #建立连接
    con=Connect(host,port,timeout,user,password)
    #执行SQL语句，查询Point表中，GN为"W3.NODE.X1","W3.NODE.X2"的ID,PN,RT,UD信息
    resultSet=con.executeQuery('select GN from Point WHERE PN="POINT4837" ')
    try:
        while resultSet.Next(): #Next()执行一次，游标下移一行
            colNum=resultSet.columnsNum  #获取列个数
            for i in range(colNum):
                colName=resultSet.columnLabel(i)    #获取第i列名字
                colValue=resultSet.getValue(i)  #获取第i列值
                print(colName,':',colValue)
    except Exception as e:
        print('error:',e)
    finally:
        resultSet.close()   #释放内存
    con.close() #



if __name__ == "__main__":

    WW_HOST = '192.168.211.36'
    WW_PORT = 8200
    WW_TIMEOUT = 60
    WW_USER = 'sis'
    WW_PASSWORD = 'openplant'

    # ServiceInfo()
    executeQuery()
