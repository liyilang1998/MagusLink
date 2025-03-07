from Scripts.OPAPI_36 import *
import time
import _thread
from app import *
import datetime


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
    resultSet=con.executeQuery('select ID,GN,AV,TM,DS from Archive where GN="W3.OPCUA.POINT4835" and TM between "2025-3-1 00:00:00" and "2025-03-2 00:00:00";')
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
    con.close() #关闭连接，千万不要忘记！！！

def Select():
    'OPIO(查询)'
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    con=Connect(host,port,timeout,user,password)
    if con.isAlive() :
        print('Connected Successful')
    else:
        print("Connect Error")
    tableName='Point'
    # colNames=['ID','PN','ND','ED']
    # keys=['1025']
    colNames=('ID','PN','ND','ED','EU')
    keys=(1025,1025)
    resultSet=con.select(tableName,colNames,keys)
    try:
        while resultSet.Next():
            colNum=resultSet.columnsNum
            print('getString-col:',resultSet.getString(1))
            print('getString-key:',resultSet.getString('PN'))
            print('getInt-col:',resultSet.getInt(0))
            print('getInt-key:',resultSet.getInt('ID'))
            for i in range(colNum):
                colN=resultSet.columnLabel(i)
                colV=resultSet.getValue(i)
                print(colN,':',colV,'colType:',resultSet.columnType(i),'valueType:',resultSet.valueType(i))
                print('columnLabel:',resultSet.columnLabel(i))
    except Exception as e:
        print('error:',e)
    finally:
        resultSet.close()   #释放内存

    con.close() #关闭连接，千万不要忘记！！！
    print("Connect closed")

def SelectArchive():
    '查询历史表中对应点信息'
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    con=Connect(host,port,timeout,user,password)
    tableName='Archive'
    # colNames=['ID','PN','ND','ED']
    # keys=['W3.NODE.JJ1','W3.NODE.JJ2']
    colNames=('GN','TM','AV')
    keys=('W3.OPCUA.POINT4835','W3.OPCUA.POINT4836')
    end=datetime.datetime.now()
    begin=end+datetime.timedelta(hours=-1)
    print(end,begin)
    options={'end':end,'begin':begin,'mode':'arch','interval':1,'qtype':0}

    resultSet=con.select(tableName,colNames,keys,options)
    total=0
    try:
        while resultSet.Next():
            colNum=resultSet.columnsNum
            for i in range(colNum):
                colN=resultSet.columnLabel(i)
                colV=resultSet.getValue(i)
                print(colN,':',colV)
                total+=1
            print('total:',total)
    except Exception as e:
        print('error:',e)
    finally:
        resultSet.close()
    con.close()


def ReadRealtime():
    'OPIO(查询)'
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    con = Connect(host, port, timeout, user, password)
    tableName = 'Realtime'
    keys=['W3.LONG.T95','W3.LONG.T96']
    colNames = ('ID', 'GN', 'AV')
    resultSet = con.select(tableName, colNames, keys, None)
    try:
        while resultSet.Next():
            colNum = resultSet.columnsNum
            for i in range(colNum):
                colN = resultSet.columnLabel(i)
                print ('col name:', colN)
                colV = resultSet.getValue(i)
                print (colN, ':', colV, 'colType:', resultSet.columnType(i), 'valueType:', resultSet.valueType(i))
                print ('columnLabel:', resultSet.columnLabel(i))
    except Exception as e:
        print ('error:', e.message)
    finally:
        resultSet.close()  # 释放内存
    con.close()  # 关闭连接，千万不要忘记！！！

if __name__=="__main__":
    # executeQuery()    #SQL(增、删、改、查)
    # Insert()  #OPIO(插入)
    # Update()  #OPIO(更新)

    #连接配置
    WW_HOST = '192.168.211.36'
    WW_PORT = 8200
    WW_TIMEOUT = 60
    WW_USER = 'sis'
    WW_PASSWORD = 'openplant'


    time1 = "2025-03-01T15:35:36"
    time2 = convert_time(time1)
    print(time2)

    # SelectArchive()
    # executeQuery()
    # Select()  #OPIO(查询)
    # SelectArchive()   #OPIO(查询历史)
    # Delete()  #OPIO(删除)
    # DeleteArchive()   #OPIO(删除历史)
    # async_()   #异步订阅
    # print('async end')

    # InsertRealtime()
    # ReadRealtime()



    # raw_input()
    # time.sleep(300)
    time.sleep(20)
    print('sleep end')

    
    

    
    

    
    



