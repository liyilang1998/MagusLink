# 数据库连接配置
DB_CONFIG = {
    'HOST': '10.25.29.12',
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
    'AX': 4,# 作为float
    'DX': 11,# 作为布尔值
    'I2': 2,# 作为Int16
    'I4': 3,#  作为Int32
    'R8': 5,# 作为Double
    'Long': 20,# 作为Int64
    'Text': 8,# 作为String
    'Blob': 17# 作为Byte
    # 可以继续添加其他映射...
}

# 统计值类型映射管理 
STATISTICAL_VALUE_TYPE_MAP = {
    '1': '最大值',
    '2': '最小值',
    '4': '平均值'
}

# 质量过滤映射管理
QUALITY_VALUE_TYPE_MAP = {
    'ALL': 0, # 不过滤
    'OnlyGood': 1, # 去除坏点
    'OnlyTime': 2, # 去除超时
    'OnlyHalf': 3 # 去除1/2
}
