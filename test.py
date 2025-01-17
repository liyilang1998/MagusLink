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
        # 格式化为指定时间格式
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

    # 请求字符串中的位号名称列表，注意需要传入全局位号名称
    keys=['W3.OPCUA.POINT4837','W3.OPCUA.POINT4831','W3.OPCUA.POINT4832','W3.OPCUA.POINT4833','W3.OPCUA.POINT4834']

    # name description unit valuetype tagID engHigh engLow
    colNames = ('PN', 'ED', 'EU', 'RT', 'ID', 'TV', 'BV')
    # 定义返回的列表和字典
    RevList = []
    RevDic = {}
    resultSet = con.select(tableName, colNames, keys)
    try:
        while resultSet.Next():
            RevDic['name'] = resultSet.getString('GN')
            RevDic['description'] = resultSet.getString('ED')
            RevDic['unit'] = resultSet.getString('EU')
            RevDic['valuetype'] = resultSet.getString('RT')
            RevDic['tagID'] = resultSet.getString('ID')
            RevDic['engHigh'] = resultSet.getString('TV')
            RevDic['engLow'] = resultSet.getString('BV')
            RevList.append(RevDic)

    except Exception as e:
        print('error:', e)


    finally:
        resultSet.close()  # 释放内存
        # print(RevList)
    con.close()  # 关闭连接，千万不要忘记！！！
    print("Connect closed")
    return RevList

if __name__ == "__main__":

    WW_HOST = '192.168.211.36'
    WW_PORT = 8200
    WW_TIMEOUT = 60
    WW_USER = 'sis'
    WW_PASSWORD = 'openplant'

    # ServiceInfo()
    # TagGet()
    a = TagGet()
    print(a)
    # print(Connected())