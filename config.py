# 数据库连接配置
DB_CONFIG = {
    'HOST': '192.168.211.36',
    'PORT': 8200,
    'TIMEOUT': 60,
    'USER': 'sis',
    'PASSWORD': 'openplant'
}

# Flask应用配置
APP_CONFIG = {
    'HOST': '0.0.0.0',
    'PORT': 9201
}

# 值类型映射关系
VALUE_TYPE_MAP = {
    '0': 'AX',#'AX'
    '1': 'DX',#'DX'
    '2': '2',#'Int16'
    '3': '3',#'Int32'
    '4': '4',#'Float'
    '5': 'Long',#'Long'
    '6': 'Text',#'Text'
    '7': '11'#'BLOB'
    # 可以继续添加其他映射...
}

# 统计值类型映射管理 
STATISTICAL_VALUE_TYPE_MAP = {
    '1': '最大值',
    '2': '最小值',
    '4': '平均值'
}