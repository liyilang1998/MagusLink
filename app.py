from flask import Flask, request, Response, jsonify
from Scripts.OPAPI_36 import *
import time
import _thread
from datetime import datetime, timezone, timedelta
from collections import OrderedDict

app = Flask(__name__)
# 数据库连接配置
WW_HOST = '192.168.211.36'
WW_PORT = 8200
WW_TIMEOUT = 60
WW_USER = 'sis'
WW_PASSWORD = 'openplant'


@app.route('/')
def hello_world():
    return 'Cybstar.MagusDataLink by yilangli@cybstar.com'

# @app.route('/Service/Info',methods=['GET'])
@app.route('/Service/<path>', methods=['GET'])
def Svrindex(path):
    if path == 'Info':
        try:
            Info = ServiceInfo()
            return jsonify(Info),200
        except Exception as e:
            print(e)
            return "Error processing Info", 500
    elif path == 'Connected':
        try:
            liveS = Connected()
            return jsonify(liveS),200
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
            return jsonify(TagInfoList)
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
    # 接收配置信息
    host = WW_HOST
    port = WW_PORT
    timeout = WW_TIMEOUT
    user = WW_USER
    password = WW_PASSWORD

    con = Connect(host, port, timeout, user, password)
    if con.isAlive():
        print('Connected Successful')
    else:
        print("Connect Error")
        return jsonify(False), 200

    con = Connect(host, port, timeout, user, password)
    # name description unit valuetype tagID engHigh engLow
    # colNames = ('PN', 'ED', 'EU', 'RT', 'ID', 'TV', 'BV')

    resultSet = con.executeQuery('select GN,ED,EU,RT,ID,TV,BV from Point')
    RevList = []
    try:
        while resultSet.Next():
            RevDic = {}
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
    con.close()  # 关闭连接，千万不要忘记！！！
    print("Connect closed")
    # print(RevList)
    # 定义返回总数、开始下标、返回的主数据
    beginIndex = 0
    totalCount = len(RevList)
    RevFList = OrderedDict([("totalCount", totalCount),("beginIndex", beginIndex),("tags", RevList)])

    return RevFList

def TagGet():
    # 从查询字符串中获取所有key=name的值对生成字典

    TagList = request.args.getlist('name')
    # 判断如果name列表为空则返回提示
    if len(TagList) == 0:
        return jsonify('Please enter at least one tagname'), 200
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
    # keys取GN（全局名称）列表
    keys = TagList
    # name description unit valuetype tagID engHigh engLow
    colNames = ('GN', 'ED', 'EU', 'RT', 'ID', 'TV', 'BV')
    # 定义返回的列表和字典
    RevList = []

    resultSet = con.select(tableName, colNames, keys)
    try:
        while resultSet.Next():
            RevDic = {}
            RevDic['name'] = resultSet.getValue('GN')
            RevDic['description'] = resultSet.getValue('ED')
            RevDic['unit'] = resultSet.getValue('EU')
            RevDic['valuetype'] = resultSet.getValue('RT')
            RevDic['tagID'] = resultSet.getValue('ID')
            RevDic['engHigh'] = resultSet.getValue('TV')
            RevDic['engLow'] = resultSet.getValue('BV')
            RevList.append(RevDic)
    except Exception as e:
        print('error:', e)
    finally:
        resultSet.close()  # 释放内存
    con.close()  # 关闭连接，千万不要忘记！！！
    print("Connect closed")
    # print(RevList)
    return RevList

@app.route('/Data/<path>', methods=['GET'])
def Dataindex(path):
    if path == 'SnapShot':
        try:
            RevInfo = SnapShot()
            return jsonify(RevInfo)
        except Exception as e:
            print(e)
            return "Error processing Info", 500
    elif path == 'HisValue':
        try:
            RevInfo = HisValue()
            return RevInfo
        except Exception as e:
            print(e)
            return "Error processing Connected", 500
    elif path == 'RawHisValue':
        try:
            RevInfo = RawHisValue()
            return RevInfo
        except Exception as e:
            print(e)
            return "Error processing Connected", 500
    elif path == 'InterpolatedHisValue':
        try:
            RevInfo = InterpolatedHisValue()
            return RevInfo
        except Exception as e:
            print(e)
            return "Error processing Connected", 500
    elif path == 'HisStaticalValue':
        try:
            RevInfo = HisStaticalValue()
            return RevInfo
        except Exception as e:
            print(e)
            return "Error processing Connected", 500
    else:
        return 'Not Found', 404

def SnapShot():
    # 从查询字符串中获取所有key=name的值对生成字典

    TagList = request.args.getlist('name')
    # 判断如果name列表为空则返回提示
    if len(TagList) == 0:
        return jsonify('Please enter at least one tagname'), 200
    # 接收配置信息
    host = WW_HOST
    port = WW_PORT
    timeout = WW_TIMEOUT
    user = WW_USER
    password = WW_PASSWORD

    con = Connect(host, port, timeout, user, password)
    if con.isAlive():
        print('Connected Successful')
    else:
        print("Connect Error")
        return jsonify(False), 200
    tableName = 'Realtime'
    # keys取GN（全局名称）列表
    keys = TagList
    # name result timeStamp status value
    colNames = ('GN', 'ID', 'TM', 'DS', 'AV')
    # 定义返回的列表和字典
    RevList = []

    resultSet = con.select(tableName, colNames, keys)
    try:
        while resultSet.Next():
            RevDic = {}
            RevDic['name'] = resultSet.getValue('GN')
            RevDic['result'] = resultSet.getValue('ID')
            RevDic['timeStamp'] = resultSet.getValue('TM')
            RevDic['status'] = resultSet.getValue('DS')
            RevDic['value'] = resultSet.getValue('AV')
            RevList.append(RevDic)
    except Exception as e:
        print('error:', e)
    finally:
        resultSet.close()  # 释放内存
    con.close()  # 关闭连接，千万不要忘记！！！
    print("Connect closed")
    # print(RevList)
    return RevList
    # return 'SnapShot',200

def HisValue():

    return 'HisValue', 200
def RawHisValue():

    return 'RawHisValue', 200
def InterpolatedHisValue():

    return 'InterpolatedHisValue', 200
def HisStaticalValue():

    return 'HisStaticalValue', 200


if __name__ == '__main__':


    app.run()
