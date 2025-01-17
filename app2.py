from flask import Flask, request, Response, jsonify
from Scripts.OPAPI_36 import *
import time
import _thread
from datetime import datetime, timezone, timedelta

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
    # 获取单个参数值
    # query = request.args.get('q')
    # 获取多个同名参数的所有值
    categories = request.args.getlist('name')
    for i in categories:
        print(i)
    # length = len(list(request.args.items()))
    # print("length: ", length)
    # TagDict = dict(request.args.items())
    # print(f"TagDict:{TagDict}")


    return "Rev!"
    # 接收配置信息
    # host=WW_HOST
    # port=WW_PORT
    # timeout=WW_TIMEOUT
    # user=WW_USER
    # password=WW_PASSWORD
    #
    # con = Connect(host, port, timeout, user, password)
    # if con.isAlive():
    #     print('Connected Successful')
    # else:
    #     print("Connect Error")
    #     return jsonify(False), 200
    # tableName = 'Point'
    # #
    # keys = list(TagDict.values())
    # # name description unit valuetype tagID engHigh engLow
    # colNames = ('PN', 'ED', 'EU', 'RT', 'ID', 'TV', 'BV')
    # # 定义返回的列表和字典
    # RevList = []
    # RevDic = {}
    # resultSet = con.select(tableName, colNames, keys)
    # try:
    #     while resultSet.Next():
    #         RevDic['name'] = resultSet.getString('GN')
    #         RevDic['description'] = resultSet.getString('ED')
    #         RevDic['unit'] = resultSet.getString('EU')
    #         RevDic['valuetype'] = resultSet.getString('RT')
    #         RevDic['tagID'] = resultSet.getString('ID')
    #         RevDic['engHigh'] = resultSet.getString('TV')
    #         RevDic['engLow'] = resultSet.getString('BV')
    #         RevList.append(RevDic)
    # except Exception as e:
    #     print('error:', e)
    # finally:
    #     resultSet.close()  # 释放内存
    # con.close()  # 关闭连接，千万不要忘记！！！
    # print("Connect closed")
    # # print(RevList)
    # return RevList




if __name__ == '__main__':

    app.run(host='127.0.0.1', port=8080, debug=True)
