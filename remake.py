from flask import Flask, request, jsonify, make_response
from datetime import *
from dateutil import parser
from typing import Tuple

# 定义全局的Flask应用对象
app = Flask(__name__)

# 尝试江字符串转换成datetime
def try_parse_time(ts: str) -> Tuple[bool, datetime]:
    if ts is None:
        return False, datetime.min
    # 尝试转换
    try:
        to = parser.parse(ts, fuzzy=False)
        return True, to
    except ValueError:
        return False, datetime.min

# 获取读取历史区间数据时候的公共参数
def get_read_hisvalues_params():
    # 获取位号名称
    tag_name = request.args.get('name')
    # 位号名称不能为空
    if tag_name is None or len(tag_name) <= 0:
        return make_response('未指定位号', 400)
    # 获取读取的历史时间范围
    sr, start_time = try_parse_time(request.args.get('start'))
    er, end_time = try_parse_time(request.args.get('end'))
    if not sr or start_time.year < 2000:
        return make_response('无效的起始时间', 400)
    if not er or end_time.year < 2000:
        return make_response('无效的结束时间', 400)
    if (end_time - start_time).total_seconds() <= 1.0:
        return make_response('无效的时间范围', 400)
    # 可选参数：取值选项
    option = 0
    if 'option' in request.args.keys():
        option = int(request.args.get('option'))


# 控制器响应函数: Service/Info 服务器信息
@app.route('/Service/Info', methods=["GET"])
def service_info():
    return jsonify({'Name': 'UnifyDataAPI-Python', 'ServerOS': 'Windows Server 2016', 'ServerTime':str(datetime.now())})


# 控制器响应函数: Service/Connected 服务是否连接到了底层数据源
@app.route('/Service/Connected', methods=["GET"])
def service_connected():
    return 'true'

# 控制器响应函数: Service/Support 服务所支持的各类操作
@app.route('/Service/Support', methods=["GET"])
def service_support():
    browser_tag = False
    # Flask的jsonify方法能够将一个dic对象转换成json对象
    return jsonify({'BrowseTag': browser_tag, 'ReadTag': True, 'WriteTag':False, 'InterpolateHis': True, 'ReadRawHis': True, 'ReadHisStatics': True})

# 控制器响应函数: /Data/Snapshot 获取一批位号的实时值
@app.route('/Data/Snapshot', methods=["GET"])
def data_snapshot_get():
    # 获取位号名称列表
    tag_names = request.args.getlist('name')
    # 位号名称列表不能为空
    if tag_names is None or len(tag_names) <= 0:
        return make_response('未指定位号', 400)
    else:
        print('Tag count = ' + str(len(tag_names)))
        index = 1
        for tag_name in tag_names:
            print(' {0} {1}'.format(index, tag_name))
            index += 1
        # 从底层数据源读取列表中位号的值
        return 'OK'

# 控制器响应函数: /Data/Snapshot 获取一批位号的实时值
@app.route('/Data/Snapshot', methods=["POST"])
def data_snapshot_post():
    # 获得POST请求中的body体并转换成json对象
    tag_names = request.get_json(force=True, cache=True)
    # 获取位号名称列表
    if tag_names is None or len(tag_names) <= 0:
        return make_response('未指定位号', 400)
    else:
        print('Tag count = ' + str(len(tag_names)))
        index = 1
        for tag_name in tag_names:
            print(' {0} {1}'.format(index, tag_name))
            index += 1
        # 从底层数据源读取列表中位号的值
        return 'OK'

# 控制器响应函数: /Data/HisValue 获取位号在指定时间点的历史数据
@app.route('/Data/HisValue', methods=["GET"])
def data_hisvalue_get():
    # 获取位号名称列表
    tag_names = request.args.getlist('name')
    # 位号名称列表不能为空
    if tag_names is None or len(tag_names) <= 0:
        return make_response('未指定位号', 400)
    # 获取读取的历史时间
    result, his_time = try_parse_time(request.args.get('time'))
    if not result or his_time.year < 2000:
        return make_response('无效的读取时间', 400)
    return 'OK'

# 控制器响应函数: /Data/RawHisValue 获取位号在指定时间区间内的历史原始值
@app.route('/Data/RawHisValue', methods=["GET"])
def data_raw_hisvalues_get():
    # 获取位号名称
    tag_name = request.args.get('name')
    # 位号名称列表不能为空
    if tag_name is None or len(tag_name) <= 0:
        return make_response('未指定位号', 400)
    # 获取读取的历史时间范围
    sr, start_time = try_parse_time(request.args.get('start'))
    er, end_time = try_parse_time(request.args.get('end'))
    if not sr or start_time.year < 2000:
        return make_response('无效的起始时间', 400)
    if not er or end_time.year < 2000:
        return make_response('无效的结束时间', 400)
    if (end_time - start_time).total_seconds() <= 1.0:
        return make_response('无效的时间范围', 400)
    # 可选参数：单次返回的最大记录数
    max_record_count = 100000
    if 'maxRecordsCount' in request.args.keys():
        max_record_count = int(request.args.get('maxRecordsCount'))
    # 打印出参数用于调试
    # print('Tag={0} {1}->{2} MaxRecords={3}'.format(tag_name, start_time, end_time, max_record_count))

    return 'OK'

# 控制器响应函数: /Data/InterpolatedHisValue 获取位号在指定时间区间内的历史数据取样值
@app.route('/Data/InterpolatedHisValue', methods=["GET"])
def data_interpolated_hisvalues_get():
    # 获取位号名称
    tag_name = request.args.get('name')
    # 位号名称不能为空
    if tag_name is None or len(tag_name) <= 0:
        return make_response('未指定位号', 400)
    # 取样间隔
    if 'span' not in request.args.keys():
        return make_response('未指定取样间隔', 400)
    span = int(request.args.get('option'))
    if span < 100:
        return make_response('无效的取样间隔', 400)
    # 获取读取的历史时间范围
    sr, start_time = try_parse_time(request.args.get('start'))
    er, end_time = try_parse_time(request.args.get('end'))
    if not sr or start_time.year < 2000:
        return make_response('无效的起始时间', 400)
    if not er or end_time.year < 2000:
        return make_response('无效的结束时间', 400)
    if (end_time - start_time).total_seconds() <= span * 1000:
        return make_response('无效的时间范围', 400)
    # 可选参数：取值选项
    option = 0
    if 'option' in request.args.keys():
        option = int(request.args.get('option'))
    # 打印出参数用于调试
    print('Tag={0} {1}->{2} span={3} option={4}'.format(tag_name, start_time, end_time, span, option))

    return 'OK'

# 控制器响应函数: /Data/HisStatisticalValue 获取位号在指定时间区间内的历史数据统计值
@app.route('/Data/HisStatisticalValue', methods=["GET"])
def data_statistical_hisvalues_get():
    # 获取位号名称
    tag_name = request.args.get('name')
    # 位号名称不能为空
    if tag_name is None or len(tag_name) <= 0:
        return make_response('未指定位号', 400)
    # 获取读取的历史时间范围
    sr, start_time = try_parse_time(request.args.get('start'))
    er, end_time = try_parse_time(request.args.get('end'))
    if not sr or start_time.year < 2000:
        return make_response('无效的起始时间', 400)
    if not er or end_time.year < 2000:
        return make_response('无效的结束时间', 400)
    if (end_time - start_time).total_seconds() <= 1.0:
        return make_response('无效的时间范围', 400)
    # 可选参数：取值选项
    option = 0
    if 'option' in request.args.keys():
        option = int(request.args.get('option'))
    # 打印出参数用于调试
    print('Tag={0} {1}->{2} option={3}'.format(tag_name, start_time, end_time, option))

    return 'OK'

# 入口函数
if __name__ == '__main__':
    print('WebSerer Demo started')
    # 启动Flask APP服务，监听8000端口
    app.run(host="127.0.0.1", port=8008, debug=True)