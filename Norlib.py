from Scripts.OPAPI_36 import *
from datetime import datetime, timezone
from config import DB_CONFIG, APP_CONFIG, VALUE_TYPE_MAP, STATISTICAL_VALUE_TYPE_MAP  # 导入配置和值类型映射

from typing import Union
import re

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
                # print('Connected Successful')
                return True
            else:
                # print("Connect Error")
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

def convert_time(
    time_input: Union[str, datetime],
    *,
    strict_timezone: bool = True,
    force_timezone: bool = False,
    default_timezone: datetime.tzinfo = timezone.utc,
    include_microseconds: bool = True,
) -> Union[datetime, str]:
    """
    时间格式双向转换（字符串 ↔ datetime），支持时区自动处理

    :param time_input: 输入时间（字符串或datetime对象）
    :param strict_timezone: 是否强制校验时区存在性（默认True）
    :param force_timezone: datetime转字符串时是否强制添加时区，若无时区则用本地时区（默认False）
    :param default_timezone: 无时区时的默认时区（默认UTC），当strict_timezone=False时生效
    :param include_microseconds: 是否包含微秒（默认True）
    :return: 转换后的时间（类型与输入相反）
    """
    def _get_local_timezone() -> datetime.tzinfo:
        """获取本地时区"""
        return datetime.now().astimezone().tzinfo

    try:
        # 输入为字符串 → 转换为datetime对象
        if isinstance(time_input, str):
            # 预处理字符串
            normalized = (
                time_input.strip()
                .replace("Z", "+00:00")
                .replace(" ", "T", 1)  # 仅替换第一个空格为T
            )
            
            # 正则校验基础格式
            if not re.match(
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:?\d{2}|Z)?$",
                normalized,
                re.IGNORECASE,
            ):
                raise ValueError(f"Invalid ISO format: {time_input}")
            # 解析为datetime对象
            dt = datetime.fromisoformat(normalized)
            # 时区处理逻辑
            if dt.tzinfo is None:
                if strict_timezone:
                    raise ValueError("输入时间缺少时区且 strict_timezone=True")
                # dt = dt.replace(tzinfo=default_timezone)
            return dt

        # 输入为datetime对象 → 转换为ISO字符串
        elif isinstance(time_input, datetime):
            dt = time_input
            
            # 时区强制处理
            if force_timezone:
                if dt.tzinfo is None:
                    local_tz = _get_local_timezone()
                    dt = dt.replace(tzinfo=local_tz)
            else:
                if strict_timezone and dt.tzinfo is None:
                    raise ValueError("datetime无时区且 strict_timezone=True")
            # 构建ISO字符串
            iso_str = dt.isoformat()
            # 微秒处理
            if not include_microseconds:
                iso_str = re.sub(r"\.\d+", "", iso_str)
            # 移除UTC的+00:00简化为Z（可选）
            if dt.tzinfo == timezone.utc:
                iso_str = iso_str.replace("+00:00", "Z")
            return iso_str
        else:
            raise TypeError("输入类型必须是字符串或datetime对象")
    
    except ValueError as e:
        raise ValueError(f"时间转换失败: {str(e)}") from e
    except AttributeError as e:
        raise TypeError(f"无效的时区类型: {str(e)}") from e
    
def decode_Ds(value):
    binary = format(value & 0xFFFF, '016b')  # 转为16位二进制补码
    print(f"二进制: {binary}")
    flags = {
        0: "开关量值",
        1: "报警位",
        2: "报警位",
        3: "报警位",
        4: "报警位",
        5: "报警未确认",
        6: "报警抑制",
        7: "是否报警",
        8: "设备强制状态",
        9: "质量好坏",
        12: "有控制指令",
        13: "设备挂牌",
        14: "初始值",
        15: "超时"
    }
    for bit in flags:
        if binary[15 - bit] == '1':  # 注意二进制字符串顺序
            print(f"bit {bit}: {flags[bit]}")

