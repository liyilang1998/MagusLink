from flask import Flask,jsonify
from Scripts.OPAPI_36 import *
import time
import _thread
from datetime import datetime, timezone, timedelta



def ServiceInfo():
    name = "UnifyDataAPI.Magus"

    try:
    # 获取操作系统名称和版本
        os_name = platform.system()
        os_version = platform.version()
        os_release = platform.release()

        serverOS = "Microsoft " +  os_name + " " + os_release + " " + os_version
        # print(serverOS)

        # 获取当前服务器时间
        current_time = datetime.now()
        current_time_with_tz = current_time.astimezone(timezone(timedelta(hours=8)))
        # 格式化为指定格式
        formatted_time = current_time_with_tz.isoformat()
        # print(formatted_time)
    except Exception as e:
        print(f"Error:{e}")

    Info_dic = {
        "name":name,
        "serverOS":serverOS,
        "serverTime":formatted_time
    }

    return Info_dic

def Connected():
    # 接收配置信息
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    try:
        con=Connect(host,port,timeout,user,password)
        if con.isAlive():
            return True
        else:
            return False
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500


def TagGet():
    # 从查询字符串中获取所有键值对生成字典
    # TagDict = dict(request.args.items())

    # 接收配置信息
    host=WW_HOST
    port=WW_PORT
    timeout=WW_TIMEOUT
    user=WW_USER
    password=WW_PASSWORD

    con = Connect(host, port, timeout, user, password)
    if con.isAlive():
        print('Connected Successful')
    else:
        print("Connect Error")
        return jsonify(False), 200
    tableName = 'Point'
    # colNames=['ID','PN','ND','ED']
    keys=['W3.OPCUA.POINT4837','W3.OPCUA.POINT4831','W3.OPCUA.POINT4832','W3.OPCUA.POINT4833','W3.OPCUA.POINT4834']
    colNames = ('ID', 'PN', 'ND', 'ED', 'EU')

    # keys = (1025, 1025)


    resultSet = con.select(tableName, colNames, keys)
    try:
        while resultSet.Next():
            colNum = resultSet.columnsNum
            print('getString-col:', resultSet.getString(1))
            print('getString-key:', resultSet.getString('PN'))
            print('getInt-col:', resultSet.getInt(0))
            print('getInt-key:', resultSet.getInt('ID'))
            for i in range(colNum):
                colN = resultSet.columnLabel(i)
                colV = resultSet.getValue(i)
                print(colN, ':', colV, 'colType:', resultSet.columnType(i), 'valueType:', resultSet.valueType(i))
                print('columnLabel:', resultSet.columnLabel(i))
    except Exception as e:
        print('error:', e)
    finally:
        resultSet.close()  # 释放内存

    con.close()  # 关闭连接，千万不要忘记！！！
    print("Connect closed")

if __name__ == "__main__":

    WW_HOST = '192.168.211.36'
    WW_PORT = 8200
    WW_TIMEOUT = 60
    WW_USER = 'sis'
    WW_PASSWORD = 'openplant'

    # ServiceInfo()
    TagGet()
    # print(Connected())