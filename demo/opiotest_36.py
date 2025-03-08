from Scripts.OPAPI_36 import *
import time
import _thread



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
    resultSet=con.executeQuery('select count(ID) from Point')
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

def Insert():
    'OPIO(插入)'
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    con=Connect(host,port,timeout,user,password)    #建立连接
    tableName='Point'
    colNames=['GN','RT','ED']   #列集合
    #插入两个点
    rows=[['W3.NODE.X7',2,'THIS IS TEST7'],['W3.NODE.X8',2,'THIS IS TEST8'],['W3.NODE.X9',2,'THIS IS TEST9']]
    resultSet=con.insert(tableName,colNames,rows)   #执行插入
    try:
        if not resultSet.isHaveWall():
            while resultSet.Next():
                colNum = resultSet.columnsNum
                for i in range(colNum):
                    colN = resultSet.columnLabel(i)
                    colV = resultSet.getValue(i)
                    print (colN, ':', colV)
        else:
            print('穿透隔离器成功')
    except Exception as e:
        print('error:',e)
    finally:
        resultSet.close()   #释放内存
    con.close() #关闭连接，千万不要忘记！！！

def Update():
    'OPIO(更新)'
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    con=Connect(host,port,timeout,user,password)
    tableName='Point'
    colNames=['GN','ED']
    rows=[['W3.NODE.X7','THIS IS TEST7'],['W3.NODE.X8','THIS IS TEST8'],['W3.NODE.X9','THIS IS TEST9']]
    resultSet=con.update(tableName,colNames,rows)
    try:
        if not resultSet.isHaveWall():
            while resultSet.Next():
                colNum = resultSet.columnsNum
                for i in range(colNum):
                    colN = resultSet.columnLabel(i)
                    colV = resultSet.getValue(i)
                    print (colN, ':', colV)
        else:
            print('穿透隔离器成功')
    except Exception as e:
        print('error:',e)
    finally:
        resultSet.close()   #释放内存
    con.close() #关闭连接，千万不要忘记！！！

def Delete():
    'OPIO(删除)'
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    con=Connect(host,port,timeout,user,password)
    tableName='Point'
    colNames=['GN']
    # keys=[200021,200022]
    keys=['W3.NODE.AA','W3.NODE.XU']
    resultSet=con.delete(tableName,colNames,keys)
    try:
        while resultSet.Next():
            colNum=resultSet.columnsNum
            for i in range(colNum):
                colN=resultSet.columnLabel(i)
                colV=resultSet.getValue(i)
                print(colN,':',colV)
    except Exception as e:
        print('error:',e)
    finally:
        resultSet.close()   #释放内存
    con.close() #关闭连接，千万不要忘记！！！

def DeleteArchive():
    'OPIO(删除历史)'
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    con=Connect(host,port,timeout,user,password)
    tableName='Archive'
    colNames=['GN']
    # keys=[200021,200022]
    keys=['W3.SYS.RATE']
    options={'begin':'2024-07-01 10:00:00','end':'2024-07-01 11:00:00'}

    resultSet=con.delete(tableName,colNames,keys,options)
    try:
        while resultSet.Next():
            colNum=resultSet.columnsNum
            for i in range(colNum):
                colN=resultSet.columnLabel(i)
                colV=resultSet.getValue(i)
                print(colN,':',colV)
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
    tableName='Stat'
    # colNames=['ID','PN','ND','ED']
    # keys=['W3.NODE.JJ1','W3.NODE.JJ2']
    colNames=('GN','TM','AV')
    keys=('W3.OPCUA.POINT4836')
    end=datetime.datetime.now()
    begin=end+datetime.timedelta(hours=-1)
    options={'end':end,'begin':begin,'qtype':0}

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

def SelectArchive2():

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
    keys=('W3.OPCUA.POINT4836','W3.OPCUA.POINT4837')
    end=datetime.datetime.now()
    begin=end+datetime.timedelta(hours=-1)
    options={'end':end,'begin':begin,'mode':'max','qtype':0}

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


    # Select()  #OPIO(查询)
    SelectArchive()   #OPIO(查询历史)
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

    
    

    
    

    
    



