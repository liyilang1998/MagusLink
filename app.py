from flask import Flask, request, Response, jsonify
from Scripts.OPAPI_36 import *
import time
import _thread
from datetime import datetime, timezone, timedelta

app = Flask(__name__)
WW_HOST = '192.168.211.36'
WW_PORT = 8200
WW_TIMEOUT = 60
WW_USER = 'sis'
WW_PASSWORD = 'openplant1'


@app.route('/')
def hello_world():
    return 'Cybstar.MagusDataLink by yilangli@cybstar.com'

# @app.route('/Service/Info',methods=['GET'])
@app.route('/Service/<path>', methods=['GET'])
def Svrindex(path):
    if path == 'Info':
        try:
            Info = ServiceInfo()
            return Info
        except Exception as e:
            print(e)
            return "Error processing Info", 500
    elif path == 'Connected':
        try:
            liveS = Connected()
            return liveS
        except Exception as e:
            print(e)
            return "Error processing Connected", 500
    else:
        return 'Not Found', 404

def ServiceInfo():
    # 定义接口名称
    name = "UnifyDataAPI.Magus"

    # 初始化变量以避免未定义的错误
    serverOS = ""
    formatted_time = ""

    try:
        # 获取操作系统名称和版本
        os_name = platform.system()
        os_version = platform.version()
        os_release = platform.release()

        serverOS = f"Microsoft {os_name} {os_release} {os_version}"

        # 获取当前服务器时间，并转换为本地时间
        current_time = datetime.now()
        current_time_with_tz = current_time.astimezone(timezone(timedelta(hours=8)))
        formatted_time = current_time_with_tz.isoformat()

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    Info_dic = {
        "name": name,
        "serverOS": serverOS,
        "serverTime": formatted_time
    }

    return jsonify(Info_dic), 200

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
            return jsonify(True), 200
        else:
            return jsonify(False), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/Tag/<path>', methods=['GET'])
def Tagindex(path):
    if path == 'Find':
        try:
            TagInfoList = TagFind()
            return TagInfoList
        except Exception as e:
            print(e)
            return "Error processing Info", 500
    elif path == 'Get':
        try:
            TagInfo = TagGet()
            return TagInfo
        except Exception as e:
            print(e)
            return "Error processing Connected", 500
    else:
        return 'Not Found', 404

def TagFind():
    return "Rev!"

def TagGet():
    # 从查询字符串中获取所有键值对生成字典
    TagDict = dict(request.args.items())

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
    # keys=['1025']
    colNames = ('ID', 'PN', 'ND', 'ED', 'EU')

    keys = (1025, 1025)


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


    return "Rev!"





if __name__ == '__main__':

    #连接配置
    # WW_HOST = '192.168.211.36'
    # WW_PORT = 8200
    # WW_TIMEOUT = 60
    # WW_USER = 'sis'
    # WW_PASSWORD = 'openplant1'



    app.run()
