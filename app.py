from flask import Flask, request, Response, jsonify
from Scripts.OPAPI_36 import *
import time
import _thread
from datetime import datetime, timezone, timedelta
from collections import OrderedDict
from config import DB_CONFIG, APP_CONFIG, VALUE_TYPE_MAP  # 导入配置和值类型映射
from typing import Tuple
import platform
import os
import sys

app = Flask(__name__)
app.json.sort_keys = False  # 防止 Flask 自动排序 JSON 键
    
# 获取程序运行时的路径
if getattr(sys, 'frozen', False):
    application_path = os.path.dirname(sys.executable)
else:
    application_path = os.path.dirname(os.path.abspath(__file__))

# 配置文件路径
config_path = os.path.join(application_path, 'config.py')

@app.route('/')
def hello_world():
    return 'Cybstar.MagusDataLink by yilangli@cybstar.com'

@app.route('/Service/<path>', methods=['GET'])
def Svrindex(path):
    if path == 'Info':
        try:
            return ServiceInfo()
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing Info"}), 500
    elif path == 'Connected':
        try:
            return Connected()
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing Connected"}), 500
    elif path == 'Support':
        try:
            return ServiceSupport()
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing Support"}), 500
    else:
        return jsonify({"error": "Not Found"}), 404

def ServiceInfo():
    # 定义接口名称
    name = "UnifyDataAPI.Magus"
    serverOS = ""
    formatted_time = ""
    try:
        # 获取操作系统信息
        os_name = platform.system()
        os_version = platform.version()
        os_release = platform.release()
        serverOS = f"{os_name} {os_release} {os_version}"
        # 获取当前服务器时间，并转换为本地时间
        current_time = datetime.now()
        formatted_time = format_datetime_with_timezone(current_time)
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500
    Info_dic = OrderedDict([
        ("name", name),
        ("serverOS", serverOS),
        ("serverTime", formatted_time)
    ])
    return jsonify(Info_dic), 200

def Connected():
    try:
        with MagusCon() as con:
            return jsonify(True), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500
    
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
    return jsonify(support_dic), 200

@app.route('/Tag/<path>', methods=['GET'])
def Tagindex(path):
    if path == 'Find':
        try:
            return TagFind()
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing Find"}), 500
    elif path == 'Get':
        try:
            return TagGet()
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing Get"}), 500
    else:
        return jsonify({"error": "Not Found"}), 404

def get_value_type(type_code):
    """
    将数值类型代码转换为对应的类型名称
    Args:
        type_code: 类型代码（字符串或数字）
    Returns:
        str: 类型名称，如果没有匹配则返回 'Unknown'
    """
    return VALUE_TYPE_MAP.get(str(type_code), 'Unknown')

def TagFind():
    # 获取查询参数
    from_index = request.args.get('From', type=int, default=0)
    count = request.args.get('Count', type=int, default=0)
    name_filter = request.args.get('name', default='')
    desc_filter = request.args.get('Description', default='')
    vt_filter = request.args.get('vt', default='')
    try:
        with MagusCon() as con:
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
                                ('valuetype', get_value_type(resultSet.getString('RT'))),
                                ('tagID', resultSet.getString('ID')),
                                ('engHigh', resultSet.getString('TV')),
                                ('engLow', resultSet.getString('BV'))
                            ])
                            RevList.append(RevDic)
                            items_added += 1
                    current_index += 1
                    # 如果已经获取到足够的数据，继续遍历以获取总数
                    if count > 0 and items_added >= count:
                        # 不要 break，继续计算总数
                        continue
            finally:
                resultSet.close()  # 确保 resultSet 被关闭
                
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    # 计算实际的起始索引
    beginIndex = min(from_index, totalCount)
    
    RevFList = OrderedDict([
        ("totalCount", totalCount),
        ("beginIndex", beginIndex),
        ("tags", RevList)
    ])

    return jsonify(RevFList), 200

def TagGet():
    # 从查询字符串中获取所有key=name的值对生成字典
    TagList = request.args.getlist('name')
    # 判断如果name列表为空则返回提示
    if TagList is None or len(TagList) <= 0:
        return jsonify({"error": "未指定位号"}), 400
    
    try:
        with MagusCon() as con:
            tableName = 'Point'
            keys = TagList
            colNames = ('GN', 'ED', 'EU', 'RT', 'ID', 'TV', 'BV')
            RevList = []

            resultSet = con.select(tableName, colNames, keys)
            try:
                while resultSet.Next():
                    RevDic = OrderedDict([
                        ('name', resultSet.getValue('GN')),
                        ('description', resultSet.getValue('ED')),
                        ('unit', resultSet.getValue('EU')),
                        ('valuetype', get_value_type(resultSet.getValue('RT'))),
                        ('tagID', resultSet.getValue('ID')),
                        ('engHigh', resultSet.getValue('TV')),
                        ('engLow', resultSet.getValue('BV'))
                    ])
                    RevList.append(RevDic)
            finally:
                resultSet.close()  # 确保 resultSet 被关闭
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify(RevList), 200

@app.route('/Data/<path>', methods=['GET', 'POST'])  # 添加 POST 方法
def Dataindex(path):
    if path == 'SnapShot':
        try:
            if request.method == 'POST':
                try:
                    tag_list = request.get_json(force=True)
                    print("tag_list:", tag_list)
                    if not isinstance(tag_list, list):
                        return jsonify({"error": "请求体必须是位号名称列表"}), 400
                except Exception as e:
                    print("JSON parsing error:", str(e))
                    return jsonify({"error": f"JSON解析错误: {str(e)}"}), 400
                
                return SnapShot(tag_list)
            else:
                return SnapShot()
        except Exception as e:
            print("Error in Dataindex:", str(e))
            return jsonify({"error": f"Error processing Info: {str(e)}"}), 500
    elif path == 'HisValue':
        try:
            return HisValue()
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing HisValue"}), 500
    elif path == 'RawHisValue':
        try:
            RevInfo = RawHisValue()
            return RevInfo
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing RawHisValue"}), 500
    elif path == 'InterpolatedHisValue':
        try:
            RevInfo = InterpolatedHisValue()
            return RevInfo
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing InterpolatedHisValue"}), 500
    elif path == 'HisStaticalValue':
        try:
            RevInfo = HisStaticalValue()
            return RevInfo
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing HisStaticalValue"}), 500
    else:
        return jsonify({"error": "Not Found"}), 404

def SnapShot(post_tags=None):
    # 获取标签列表
    if post_tags is not None:
        TagList = post_tags
    else:
        TagList = request.args.getlist('name')
    
    if TagList is None or len(TagList) <= 0:
        return jsonify({"error": "未指定位号"}), 400
        
    try:
        with MagusCon() as con:
            tableName = 'Realtime'
            colNames = ('GN', 'ID', 'TM', 'DS', 'AV')
            resultSet = con.select(tableName, colNames, TagList)
            
            RevList = []
            try:
                while resultSet.Next():
                    # 获取时间（datetime）并转换格式
                    timestamp = resultSet.getDateTime('TM')
                    
                    # 处理 name 和 result
                    name = resultSet.getValue('GN')
                    result = 0 if name and name.strip() else 1
                    
                    # 处理 status
                    status_value = resultSet.getString('DS')
                    status = "192" if status_value == "0" else "-1"
                    
                    formatted_time = format_datetime_with_timezone(timestamp)
                    
                    # 使用 OrderedDict 确保字段顺序
                    RevDic = OrderedDict([
                        ('name', name),
                        ('result', result),
                        ('timeStamp', formatted_time),
                        ('status', status),
                        ('value', resultSet.getString('AV'))
                    ])
                    RevList.append(RevDic)
            finally:
                resultSet.close()  # 确保 resultSet 被关闭
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify(RevList), 200

def HisValue():
    # 获取查询参数
    TagList = request.args.getlist('name')  # 获取位号列表
    time_str = request.args.get('time')     # 获取时间参数
    
    # 判断参数是否有效
    if TagList is None or len(TagList) <= 0:
        return jsonify({"error": "未指定位号"}), 400
    if not time_str:
        return jsonify({"error": "未指定时间"}), 400
        
    try:
        # 将时间字符串转换为datetime对象
        query_time = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except ValueError as e:
        return jsonify({"error": "时间格式无效"}), 400
    
    try:
        with MagusCon() as con:
            tableName = 'History'  # 历史数据表名
            keys = TagList
            colNames = ('GN', 'ID', 'TM', 'DS', 'AV')  # 与实时数据相同的列
            RevList = []

            # 使用指定时间查询历史数据
            resultSet = con.select(tableName, colNames, keys, query_time)
            try:
                while resultSet.Next():
                    # 获取时间并转换格式
                    timestamp = resultSet.getDateTime('TM')
                    
                    # 处理 name 和 result
                    name = resultSet.getValue('GN')
                    result = 0 if name and name.strip() else 1
                    
                    # 处理 status
                    status_value = resultSet.getString('DS')
                    status = "192" if status_value == "0" else "-1"
                    
                    formatted_time = format_datetime_with_timezone(timestamp, query_time)
                    
                    # 使用 OrderedDict 确保字段顺序
                    RevDic = OrderedDict([
                        ('name', name),
                        ('result', result),
                        ('timeStamp', formatted_time),
                        ('status', status),
                        ('value', resultSet.getString('AV'))
                    ])
                    RevList.append(RevDic)
            finally:
                resultSet.close()  # 确保 resultSet 被关闭
                
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify(RevList), 200

def RawHisValue():

    return 'RawHisValue', 200
def InterpolatedHisValue():

    return 'InterpolatedHisValue', 200
def HisStaticalValue():

    return 'HisStaticalValue', 200

def format_datetime_with_timezone(timestamp, default_time=None):
    """
    将时间戳转换为带时区的ISO格式字符串
    Args:
        timestamp: datetime对象或可转换为datetime的值
        default_time: 当转换失败时使用的默认时间
    Returns:
        str: 格式化的时间字符串 (例如: "2025-01-21T13:33:43.908+08:00")
    """
    try:
        if isinstance(timestamp, datetime):
            dt = timestamp
        else:
            dt = datetime.fromtimestamp(float(timestamp))
        
        # 转换为北京时间并格式化
        dt_beijing = dt.astimezone(timezone(timedelta(hours=8)))
        return dt_beijing.isoformat()
    except (ValueError, TypeError, AttributeError) as e:
        print(f"Time format error: {e}")
        if default_time:
            # 如果提供了默认时间，使用默认时间
            dt_beijing = default_time.astimezone(timezone(timedelta(hours=8)))
            return dt_beijing.isoformat()
        else:
            # 如果没有提供默认时间，使用当前时间
            current_time = datetime.now()
            dt_beijing = current_time.astimezone(timezone(timedelta(hours=8)))
            return dt_beijing.isoformat()

class MagusCon:
    def __init__(self):
        # 从配置中获取连接信息
        self.host = DB_CONFIG['HOST']
        self.port = DB_CONFIG['PORT']
        self.timeout = DB_CONFIG['TIMEOUT']
        self.user = DB_CONFIG['USER']
        self.password = DB_CONFIG['PASSWORD']
        self.con = None
        
    def connect(self):
        """创建数据库连接并返回连接状态"""
        try:
            self.con = Connect(self.host, self.port, self.timeout, self.user, self.password)
            if self.con.isAlive():
                print('Connected Successful')
                return True
            else:
                print("Connect Error")
                return False
        except Exception as e:
            print(f"Connection Error: {e}")
            return False
            
    def close(self):
        """关闭数据库连接"""
        if self.con:
            self.con.close()
            print("Connect closed")
            
    def __enter__(self):
        """上下文管理器入口"""
        if self.connect():
            return self.con
        raise ConnectionError("Failed to connect to database")
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.close()

if __name__ == '__main__':
    app.run(
        host=APP_CONFIG['HOST'], 
        port=APP_CONFIG['PORT']
    )
