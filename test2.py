from Scripts.OPAPI_36 import *
import time
import _thread


#通过sql查询所有位号
def TagFind():

    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    #建立连接
    con=Connect(host,port,timeout,user,password)
    if con.isAlive() :
        print('Connected Successful')
    else:
        print("Connect Failed")
    #执行SQL语句，查询Point表中，GN为"W3.NODE.X1","W3.NODE.X2"的ID,PN,RT,UD信息
    resultSet=con.executeQuery('select PN,ED,EU,RT,ID,TV,BV from Point')
    try:
        while resultSet.Next(): #Next()执行一次，游标下移一行
            colNum=resultSet.columnsNum  #获取列个数
            dict = {}
            for i in range(colNum):
                dict[resultSet.columnLabel(i)] = resultSet.getValue(i)
                # colName=resultSet.columnLabel(i)    #获取第i列名字
                # colValue=resultSet.getValue(i)  #获取第i列值
                # print(colName,':',colValue)
            print(dict)
    except Exception as e:
        print('error:',e)
    finally:
        resultSet.close()   #释放内存
    con.close()#关闭连接，千万不要忘记！！！
    print("Connection closed")

#查询指定位号的属性
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

#查询指定位号的实时值

if __name__=="__main__":


    #连接配置
    WW_HOST = '192.168.211.36'
    WW_PORT = 8200
    WW_TIMEOUT = 60
    WW_USER = 'sis'
    WW_PASSWORD = 'openplant'

    Select()
    # TagFind()
    # Select()  #OPIO(查询)

    # time.sleep(300)
    time.sleep(20)
    print('sleep end')