from flask import Flask, request, jsonify
from Scripts.OPAPI_36 import *
from datetime import datetime
from collections import OrderedDict
from config import APP_CONFIG, STATISTICAL_VALUE_TYPE_MAP, QUALITY_VALUE_TYPE_MAP  # 导入配置和值类型映射
import platform
import os, sys
from Norlib import *

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
        formatted_time = convert_time(current_time, strict_timezone=False)
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
    # 先写死，后续更新
    support_dic = OrderedDict([
        ("browseTag", True),
        ("readTag", True),
        ("writeTag", False),
        ("interpolateHis", True),
        ("readRawHis", True),
        ("readHisStatics", True)
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
    elif path == 'get':
        try:
            return TagGet()
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing Get"}), 500
    else:
        return jsonify({"error": "Not Found"}), 404
def TagFind():
    # 获取查询参数
    from_index = request.args.get('From', type=int, default=0)
    count = request.args.get('Count', type=int)
    name_filter = request.args.get('name', default='')
    desc_filter = request.args.get('description', default='')
    vt_filter = request.args.get('vt', default='')
    index_move = 0

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

            if count:
                where_clause = where_clause + f" limit {from_index},{count}"
            else:
                index_move = from_index

            # 执行查询
            query = base_query + where_clause
            print(f"Executing query: {query}")
            resultSet = con.executeQuery(query)
            all_records = []
            
            try:
                # 当
                while index_move > 0:
                    resultSet.Next()
                    index_move -= 1

                while resultSet.Next():
                    RevDic = OrderedDict([
                        ('name', resultSet.getString('GN')),
                        ('description', resultSet.getString('ED')),
                        ('unit', resultSet.getString('EU')),
                        ('valuetype', resultSet.getValue('RT')),
                        ('tagID', resultSet.getValue('ID')),
                        ('engHigh', resultSet.getValue('TV')),
                        ('engLow', resultSet.getValue('BV'))
                    ])
                    all_records.append(RevDic)
            finally:
                resultSet.close()
            # 计算总记录数
            totalCount = len(all_records)
            
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    RevFList = OrderedDict([
        ("totalCount", int(totalCount)),
        ("beginIndex", int(from_index)),
        ("tags", all_records)
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
                        ('valuetype', VALUE_TYPE_MAP.get(resultSet.getValue('RT'), -1)),
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

@app.route('/Data/<path>', methods=['GET', 'POST']) 
def Dataindex(path):
    if path == 'Snapshot':
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
    elif path == 'HisStatisticalValue':
        try:
            RevInfo = HisStatisticalValue()
            return RevInfo
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing HisStaticalValue"}), 500
    elif path == 'RawHisTrend':
        try:
            RevInfo = RawHisTrend()
            return RevInfo
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing RawHisTrend"}), 500
    elif path == 'InterpolatedHisTrend':
        try:
            RevInfo = InterpolatedHisTrend()
            return RevInfo
        except Exception as e:
            print(e)
            return jsonify({"error": "Error processing InterpolatedHisTrend"}), 500
    
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
                    timestamp1 = resultSet.getDateTime('TM')
                    name = resultSet.getValue('GN')
                    result = 0 if name and name.strip() else 1
                    
                    # 处理 status 简单判断为DS为0则正常，否则为异常
                    status_value = resultSet.getString('DS')
                    status = 192 if status_value == "0" else 0
                    
                    formatted_time = convert_time(timestamp1, strict_timezone=False, include_microseconds=False)
                    
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
 
    return 'HisValue', 200
def RawHisValue():

    return 'RawHisValue', 200
def InterpolatedHisValue():
    # 获取查询参数
    name = request.args.getlist('name') 
    start_time = request.args.get('start')
    end_time = request.args.get('end')
    interval = request.args.get('span', default=60000)

    # 判断入参是否有效
    if name is None:
        return jsonify({"error": "未指定位号"}), 400
    elif len(name) > 1:
        return jsonify({"error": "非法的位号参数"}), 400
    if not start_time:
        return jsonify({"error": "未指定开始时间"}), 400
    if not end_time:
        return jsonify({"error": "未指定结束时间"}), 400
    # 时间间隔最小为5000ms(5s)
    if not int(interval) < 5000:
        interval = 5000

    # 将时间字符串转换为datetime对象
    query_time_start = convert_time(start_time, strict_timezone=False)
    query_time_end = convert_time(end_time, strict_timezone=False)

    if query_time_start > query_time_end:
        return jsonify({"error": "无效的时间范围"}), 400

    try:
        with MagusCon() as con:
            # ----获取区间统计值max min avg----
            keys_archive = name
            tableName_archive = 'Archive'
            # 拼接SQL语句
            sqlshell_archive = f'select * from {tableName_archive} where GN = "{keys_archive[0]}" and interval = {interval/1000} and TM between "{query_time_start}" and "{query_time_end}"'
            resultSet_archive = con.executeQuery(sqlshell_archive)
            # 定义原始历史数据列表
            RevDic = []
            try:
                while resultSet_archive.Next():
                    timestamp2 = resultSet_archive.getDateTime('TM')
                    formatted_time = convert_time(timestamp2, strict_timezone=False, include_microseconds=False)
                    # 处理status，返回值为0则192，否则为0
                    status_archive = 192 if resultSet_archive.getValue('DS') == 0 else 0
                    # 使用 OrderedDict 确保字段顺序
                    valueList = OrderedDict([
                        ('timeStamp', formatted_time),
                        ('status', status_archive),
                        ('value', resultSet_archive.getString('AV'))
                    ])    
                    RevDic.append(valueList)
            finally:
                resultSet_archive.close()  # 确保 resultSet 被关闭
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify(RevDic), 200
def HisStatisticalValue():
  # 获取查询参数
    name = request.args.getlist('name') 
    start_time = request.args.get('start')
    end_time = request.args.get('end')
    option = request.args.get('option')
    qtype = request.args.get('statusFilter', default="OnlyGood")

    # 判断入参是否有效
    if name is None:
        return jsonify({"error": "未指定位号"}), 400
    elif len(name) > 1:
        return jsonify({"error": "非法的位号参数"}), 400
    if not start_time:
        return jsonify({"error": "未指定开始时间"}), 400
    if not end_time:
        return jsonify({"error": "未指定结束时间"}), 400
    if not option:
        return jsonify({"error": "未指定统计值参数"}), 400
    if not option in STATISTICAL_VALUE_TYPE_MAP.keys():
        return jsonify({"error": "非法的统计值参数"}), 400
    if not qtype in QUALITY_VALUE_TYPE_MAP.keys():
        return jsonify({"error": "非法的状态过滤参数"}), 400
    
    # 将时间字符串转换为datetime对象
    query_time_start = convert_time(start_time, strict_timezone=False)
    query_time_end = convert_time(end_time, strict_timezone=False)

    if query_time_start > query_time_end:
        return jsonify({"error": "无效的时间范围"}), 400     

    try:
        with MagusCon() as con:
            # ----获取区间统计值max min avg----
            keys_stat = name
            tableName_stat = 'Archive'
            # 拼接SQL语句
            sqlshell_stat = f'SELECT * FROM {tableName_stat} WHERE GN = "{keys_stat[0]}" AND mode = "{STATISTICAL_VALUE_TYPE_MAP[option]}" AND qtype = "{QUALITY_VALUE_TYPE_MAP[qtype]}" AND TM BETWEEN "{query_time_start}" AND "{query_time_end}"'
            resultSet_stat = con.executeQuery(sqlshell_stat)
            # resultSet对象存在及为0，否则为1
            result_stat = 0 if resultSet_stat else 1

            # 定义原始历史数据列表
            RevDic = []
            try:
                while resultSet_stat.Next():
                    timestamp2 = resultSet_stat.getDateTime('TM')
                    formatted_time = convert_time(timestamp2, strict_timezone=False, include_microseconds=False)
                    # 处理status，返回值为0则192，否则为0
                    status_stat = 192 if resultSet_stat.getValue('DS') == 0 else 0
                    # 使用 OrderedDict 确保字段顺序
                    valueList = OrderedDict([
                        ('result', result_stat),
                        ('timeStamp', formatted_time),
                        ('status', status_stat),
                        ('value', resultSet_stat.getString('AV')),
                        ('option', STATISTICAL_VALUE_TYPE_MAP[option])
                    ])    
                    RevDic.append(valueList)
            finally:
                resultSet_stat.close()  # 确保 resultSet 被关闭

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify(RevDic), 200
def RawHisTrend():
    # 获取查询参数
    name = request.args.getlist('name') 
    start_time = request.args.get('start')
    end_time = request.args.get('end')
    qtype = request.args.get('qtype', default=0)

    # 判断入参是否有效
    if name is None:
        return jsonify({"error": "未指定位号"}), 400
    elif len(name) > 1:
        return jsonify({"error": "非法的位号参数"}), 400
    if not start_time:
        return jsonify({"error": "未指定开始时间"}), 400
    if not end_time:
        return jsonify({"error": "未指定结束时间"}), 400

    # 将时间字符串转换为datetime对象
    query_time_start = convert_time(start_time, strict_timezone=False)
    query_time_end = convert_time(end_time, strict_timezone=False)

    if query_time_start > query_time_end:
        return jsonify({"error": "无效的时间范围"}), 400

    try:
        with MagusCon() as con:
            # ----获取区间统计值max min avg----
            keys_stat = name
            tableName_stat = 'Stat'
            # 拼接SQL语句
            sqlshell_stat = f'select * from {tableName_stat} where GN = "{keys_stat[0]}" and qtype = "{qtype}" and TM between "{query_time_start}" and "{query_time_end}"'
            print(sqlshell_stat)
            resultSet_stat = con.executeQuery(sqlshell_stat)
            # resultSet对象存在及为0，否则为1
            result1 = 0 if resultSet_stat else 1
            # 处理status，返回值为0则192，否则为0
            status_stat = 192 if resultSet_stat.getValue('DS') == 0 else 0

            try:
                while resultSet_stat.Next():
                    RevDic = {
                        'result':result1,
                        'maxvalue':{'timeStamp':convert_time(resultSet_stat.getDateTime('MAXTIME'), strict_timezone=False), 'status':status_stat, 'value':resultSet_stat.getString('MAXV')},
                        'minvalue':{'timeStamp':convert_time(resultSet_stat.getDateTime('MINTIME'), strict_timezone=False), 'status':status_stat, 'value':resultSet_stat.getString('MINV')},
                        'avg':{'timeStamp':resultSet_stat.getDateTime('AVGTIME'),'status':status_stat,'value':resultSet_stat.getString('AVGV')}
                    }
            except Exception as e:
                print(f"Error: {e}")
                return jsonify({"error": str(e)}), 500
            
            # ----获取区间原始数据----
            tableName_archive = 'Archive'
            keys_archive = name
            colNames_archive = ('TM', 'DS', 'AV')
            options_archive = {'end':query_time_end, 'begin':query_time_start, 'qtype':0}
            trendDic = []
            resultSet_archive = con.select(tableName_archive, colNames_archive, keys_archive, options_archive)
            try:
                while resultSet_archive.Next():
                    timestamp2 = resultSet_archive.getDateTime('TM')
                    formatted_time = convert_time(timestamp2, strict_timezone=False, include_microseconds=False)

                    # 处理status，返回值为0则192，否则为0
                    status_archive = 192 if resultSet_stat.getValue('DS') == 0 else 0
                    # 使用 OrderedDict 确保字段顺序
                    valueList = OrderedDict([
                        ('timeStamp', formatted_time),
                        ('status', status_archive),
                        ('value', resultSet_archive.getString('AV'))
                    ])    
                    trendDic.append(valueList)
            finally:
                resultSet_archive.close()  # 确保 resultSet 被关闭
            # 把区间原始数据列表添加到RevDic中
            RevDic['trendValues'] = trendDic

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify(RevDic), 200
def InterpolatedHisTrend():
    # 获取查询参数
    name = request.args.getlist('name') 
    start_time = request.args.get('start')
    end_time = request.args.get('end')
    interval = request.args.get('span', default=60000)
    qtype = request.args.get('statusFilter', default="OnlyGood")

    # 判断入参是否有效
    if name is None:
        return jsonify({"error": "未指定位号"}), 400
    elif len(name) > 1:
        return jsonify({"error": "非法的位号参数"}), 400
    if not start_time:
        return jsonify({"error": "未指定开始时间"}), 400
    if not end_time:
        return jsonify({"error": "未指定结束时间"}), 400
    # 时间间隔最小为5000ms(5s)
    if not int(interval) < 5000:
        interval = 5000
    if not qtype:
        return jsonify({"error": "未指定状态过滤"}), 400
    if not qtype in QUALITY_VALUE_TYPE_MAP.keys():
        return jsonify({"error": "非法的状态过滤参数"}), 400

    # 将时间字符串转换为datetime对象
    query_time_start = convert_time(start_time, strict_timezone=False)
    query_time_end = convert_time(end_time, strict_timezone=False)

    if query_time_start > query_time_end:
        return jsonify({"error": "无效的时间范围"}), 400

    try:
        with MagusCon() as con:
            # ----获取区间统计值max min avg----
            keys_stat = name
            tableName_stat = 'Stat'
            # 拼接SQL语句
            sqlshell_stat = f'select * from {tableName_stat} where GN = "{keys_stat[0]}" and qtype = "{QUALITY_VALUE_TYPE_MAP[qtype]}" and TM between "{query_time_start}" and "{query_time_end}"'
            print(sqlshell_stat)
            resultSet_stat = con.executeQuery(sqlshell_stat)
            # resultSet对象存在及为0，否则为1
            result1 = 0 if resultSet_stat else 1
            # 处理status，返回值为0则192，否则为0
            status_stat = 192 if resultSet_stat.getValue('DS') == 0 else 0

            try:
                while resultSet_stat.Next():
                    RevDic = {
                        'result':result1,
                        'maxvalue':{'timeStamp':convert_time(resultSet_stat.getDateTime('MAXTIME'), strict_timezone=False), 'status':status_stat, 'value':resultSet_stat.getString('MAXV')},
                        'minvalue':{'timeStamp':convert_time(resultSet_stat.getDateTime('MINTIME'), strict_timezone=False), 'status':status_stat, 'value':resultSet_stat.getString('MINV')},
                        'avg':{'timeStamp':convert_time(resultSet_stat.getDateTime('MAXTIME'), strict_timezone=False),'status':status_stat,'value':resultSet_stat.getString('AVGV')}
                    }
            except Exception as e:
                print(f"Error: {e}")
                return jsonify({"error": str(e)}), 500
            
            # ----获取区间原始数据----
            tableName_archive = 'Archive'
            keys_archive = name
            colNames_archive = ('TM', 'DS', 'AV')
            options_archive = {'end':query_time_end, 'begin':query_time_start, 'interval':int(interval/1000), 'qtype':QUALITY_VALUE_TYPE_MAP[qtype]}
            
            print("interval:",int(interval/1000))
            # 定义原始历史数据列表
            trendDic = []
            resultSet_archive = con.select(tableName_archive, colNames_archive, keys_archive, options_archive)
            try:
                while resultSet_archive.Next():
                    timestamp2 = resultSet_archive.getDateTime('TM')
                    formatted_time = convert_time(timestamp2, strict_timezone=False, include_microseconds=False)
                    # 处理status，返回值为0则192，否则为0
                    status_archive = 192 if resultSet_stat.getValue('DS') == 0 else 0
                    # 使用 OrderedDict 确保字段顺序
                    valueList = OrderedDict([
                        ('timeStamp', formatted_time),
                        ('status', status_archive),
                        ('value', resultSet_archive.getString('AV'))
                    ])    
                    trendDic.append(valueList)
            finally:
                resultSet_archive.close()  # 确保 resultSet 被关闭
            # 把区间原始数据列表添加到RevDic中
            RevDic['trendValues'] = trendDic

    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": str(e)}), 500

    return jsonify(RevDic), 200
if __name__ == '__main__':

    app.run(
        host=APP_CONFIG['HOST'], 
        port=APP_CONFIG['PORT'],
        threaded=True
    )
