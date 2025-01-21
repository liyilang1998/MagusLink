from flask import Flask, request, Response, jsonify
from Scripts.OPAPI_36 import *
import time
import _thread
from datetime import datetime, timezone, timedelta
from collections import OrderedDict
from config import DB_CONFIG, APP_CONFIG  # 导入配置
from typing import Tuple

app = Flask(__name__)
app.json.sort_keys = False  # 防止 Flask 自动排序 JSON 键


# def try_parse_time(ts: str) -> Tuple[bool, datetime]:
#     if ts is None:
#         return False, datetime.min
#     # 尝试转换
#     try:
#         to = parser.parse(ts, fuzzy=False)
#         return True, to
#     except ValueError:
#         return False, datetime.min
    
    
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
    elif path == 'Support':  # 添加新的路由处理
        try:
            support = ServiceSupport()
            return jsonify(support), 200
        except Exception as e:
            print(e)
            return "Error processing Support", 500
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

        serverOS = f"{os_name} {os_release} {os_version}"

        # 获取当前服务器时间，并转换为本地时间
        current_time = datetime.now()
        current_time_with_tz = current_time.astimezone(timezone(timedelta(hours=8)))
        formatted_time = current_time_with_tz.isoformat()

    except Exception as e:
        print(f"Error: {e}")
        return OrderedDict([("error", str(e))]), 500

    Info_dic = OrderedDict([
        ("name", name),
        ("serverOS", serverOS),
        ("serverTime", formatted_time)
    ])

    return Info_dic

def Connected():
    # 从配置中获取连接信息
    host = DB_CONFIG['HOST']
    port = DB_CONFIG['PORT']
    timeout = DB_CONFIG['TIMEOUT']
    user = DB_CONFIG['USER']
    password = DB_CONFIG['PASSWORD']

    try:
        con = Connect(host, port, timeout, user, password)
        if con.isAlive():
            return True
        else:
            return False
    except Exception as e:
        print(f"Error: {e}")
        return OrderedDict([("error", str(e))]), 500
    
def ServiceSupport():
    # 后续更新
    support_dic = OrderedDict([
        ("browseTag", "UnKnow"),
        ("readTag", "UnKnow"),
        ("writeTag", "UnKnow"),
        ("interpolateHis", "UnKnow"),
        ("readRawHis", "UnKnow"),
        ("readHisStatics", "UnKnow")
    ])
    
    return support_dic

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
            return jsonify(TagInfo), 200
        except Exception as e:
            print(e)
            return "Error processing Connected", 500
    else:
        return 'Not Found', 404

def TagFind():
    # 获取查询参数
    from_index = request.args.get('From', type=int, default=0)  # 默认从0开始
    count = request.args.get('Count', type=int, default=0)      # 默认返回所有
    name_filter = request.args.get('Name', default='')          # 名称过滤
    desc_filter = request.args.get('Description', default='')   # 描述过滤
    vt_filter = request.args.get('vt', default='')             # 值类型过滤
    
    # 从配置中获取连接信息
    host = DB_CONFIG['HOST']
    port = DB_CONFIG['PORT']
    timeout = DB_CONFIG['TIMEOUT']
    user = DB_CONFIG['USER']
    password = DB_CONFIG['PASSWORD']

    con = Connect(host, port, timeout, user, password)
    if con.isAlive():
        print('Connected Successful')
    else:
        print("Connect Error")
        return jsonify(False), 400

    # 构建SQL查询语句
    base_query = 'SELECT GN,ED,EU,RT,ID,TV,BV FROM Point'
    where_conditions = []
    
    if name_filter:
        where_conditions.append(f"GN LIKE '%{name_filter}%'")
    if desc_filter:
        where_conditions.append(f"ED LIKE '%{desc_filter}%'")
    if vt_filter:
        where_conditions.append(f"RT = '{vt_filter}'")
        
    # 拼接WHERE条件
    where_clause = ''
    if where_conditions:
        where_clause = ' WHERE ' + ' AND '.join(where_conditions)
    
    # 执行查询
    query = base_query + where_clause
    print(f"Executing query: {query}")
    resultSet = con.executeQuery(query)
    RevList = []
    current_index = 0
    items_added = 0
    totalCount = 0  # 用于计算总记录数
    
    try:
        while resultSet.Next():
            totalCount += 1  # 计算总记录数
            # 只有当当前索引大于等于 from_index 时才考虑添加数据
            if current_index >= from_index:
                # 如果没有指定count或者还没有达到指定的数量，则添加数据
                if count <= 0 or items_added < count:
                    RevDic = OrderedDict([
                        ('name', resultSet.getString('GN')),
                        ('description', resultSet.getString('ED')),
                        ('unit', resultSet.getString('EU')),
                        ('valuetype', resultSet.getString('RT')),
                        ('tagID', resultSet.getString('ID')),
                        ('engHigh', resultSet.getString('TV')),
                        ('engLow', resultSet.getString('BV'))
                    ])
                    RevList.append(RevDic)
                    items_added += 1
                elif items_added >= count:
                    # 如果已经达到指定数量，可以提前退出循环
                    break
            current_index += 1
    except Exception as e:
        print('error:', e)
    finally:
        resultSet.close()  # 释放内存
    con.close()  # 关闭连接
    print("Connect closed")

    # 计算实际的起始索引
    beginIndex = min(from_index, totalCount)
    
    RevFList = OrderedDict([
        ("totalCount", totalCount),
        ("beginIndex", beginIndex),
        ("tags", RevList)
    ])

    return RevFList

def TagGet():
    # 从查询字符串中获取所有key=name的值对生成字典
    TagList = request.args.getlist('name')
    # 判断如果name列表为空则返回提示
    if TagList is None or len(TagList) <= 0:
        return '未指定位号'
    
    # 从配置中获取连接信息
    host = DB_CONFIG['HOST']
    port = DB_CONFIG['PORT']
    timeout = DB_CONFIG['TIMEOUT']
    user = DB_CONFIG['USER']
    password = DB_CONFIG['PASSWORD']

    con = Connect(host, port, timeout, user, password)
    if con.isAlive():
        print('Connected Successful')
    else:
        print("Connect Error")
        return jsonify(False), 400
        
    tableName = 'Point'
    keys = TagList
    colNames = ('GN', 'ED', 'EU', 'RT', 'ID', 'TV', 'BV')
    RevList = []

    resultSet = con.select(tableName, colNames, keys)
    try:
        while resultSet.Next():
            # 使用 OrderedDict 确保字段顺序
            RevDic = OrderedDict([
                ('name', resultSet.getValue('GN')),
                ('description', resultSet.getValue('ED')),
                ('unit', resultSet.getValue('EU')),
                ('valuetype', resultSet.getValue('RT')),
                ('tagID', resultSet.getValue('ID')),
                ('engHigh', resultSet.getValue('TV')),
                ('engLow', resultSet.getValue('BV'))
            ])
            RevList.append(RevDic)
    except Exception as e:
        print('error:', e)
    finally:
        resultSet.close()  # 释放内存
    con.close()  # 关闭连接，千万不要忘记！！！
    print("Connect closed")
    
    return RevList

@app.route('/Data/<path>', methods=['GET', 'POST'])  # 添加 POST 方法
def Dataindex(path):
    if path == 'SnapShot':
        try:
            if request.method == 'POST':
                # 添加请求信息的调试输出
                print("Content-Type:", request.headers.get('Content-Type'))
                print("Request data:", request.data)
                
                # 获取并验证 POST 请求体
                try:
                    tag_list = request.get_json(force=True)  # 添加 force=True
                    print("tag_list:", tag_list)
                    if not isinstance(tag_list, list):
                        return '请求体必须是位号名称列表', 400
                except Exception as e:
                    print("JSON parsing error:", str(e))
                    return f'JSON解析错误: {str(e)}', 400
                
                RevInfo = SnapShot(tag_list)  # 传入标签列表
            else:  # GET 方法
                RevInfo = SnapShot()
            return jsonify(RevInfo)
        except Exception as e:
            print("Error in Dataindex:", str(e))
            return f"Error processing Info: {str(e)}", 500
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

def SnapShot(post_tags=None):
    # 根据请求方法获取标签列表
    if post_tags is not None:
        TagList = post_tags  # 使用 POST 请求传入的标签列表
    else:
        TagList = request.args.getlist('name')  # GET 方法从查询参数获取
    
    # 判断如果name列表为空则返回提示
    if TagList is None or len(TagList) <= 0:
        return '未指定位号'
    
    # 从配置中获取连接信息
    host = DB_CONFIG['HOST']
    port = DB_CONFIG['PORT']
    timeout = DB_CONFIG['TIMEOUT']
    user = DB_CONFIG['USER']
    password = DB_CONFIG['PASSWORD']

    con = Connect(host, port, timeout, user, password)
    if con.isAlive():
        print('Connected Successful')
    else:
        print("Connect Error")
        return jsonify(False), 400
    
    tableName = 'Realtime'
    keys = TagList
    colNames = ('GN', 'ID', 'TM', 'DS', 'AV')
    RevList = []

    resultSet = con.select(tableName, colNames, keys)
    try:
        while resultSet.Next():
            # 获取时间（datetime）并转换格式
            timestamp = resultSet.getDateTime('TM')
            
            # 处理 name 和 result
            name = resultSet.getValue('GN')
            result = 0 if name and name.strip() else 1
            
            # 处理 status
            status_value = resultSet.getString('DS')
            # print(f"{name}-status_value:", status_value, type(status_value))
            status = "192" if status_value == "0" else "-1"
            
            try:
                # 转换为北京时间并格式化
                dt_beijing = timestamp.astimezone(timezone(timedelta(hours=8)))
                formatted_time = dt_beijing.isoformat()
            except (ValueError, TypeError, AttributeError):
                # 如果转换失败，尝试使用当前时间
                current_time = datetime.now()
                dt_beijing = current_time.astimezone(timezone(timedelta(hours=8)))
                formatted_time = dt_beijing.isoformat()
            
            # 使用 OrderedDict 确保字段顺序
            RevDic = OrderedDict([
                ('name', name),
                ('result', result),
                ('timeStamp', formatted_time),
                ('status', status),
                ('value', resultSet.getString('AV'))
            ])
            RevList.append(RevDic)
    except Exception as e:
        print('error:', e)
        return OrderedDict([("error", str(e))]), 500
    finally:
        resultSet.close()
        con.close()
        print("Connect closed")

    return RevList

def HisValue():

    return 'HisValue', 200
def RawHisValue():

    return 'RawHisValue', 200
def InterpolatedHisValue():

    return 'InterpolatedHisValue', 200
def HisStaticalValue():

    return 'HisStaticalValue', 200



if __name__ == '__main__':
    app.run(
        host=APP_CONFIG['HOST'], 
        port=APP_CONFIG['PORT']
    )
