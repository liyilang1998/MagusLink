from ctypes import *
import datetime
import time
import numpy
import platform
import sys
from pathlib import Path

main_program_path = Path(__file__).resolve().parent

if platform.system() == 'Windows':
    api = WinDLL(main_program_path / 'opapi4.dll')
elif platform.system() == 'Darwin':
    api = CDLL('libopapi4.dylib')
else:
    api = CDLL('libopapi4.so')


class IO:
    'OPAPI外部接口'

    def init(self, option, host, port, timeout, user, password, buffer_path, buffer_size):
        api.op2_init.restype = c_void_p
        api.op2_init.argtypes = [c_int, c_char_p, c_int, c_int, c_char_p, c_char_p, c_char_p, c_int]
        if isinstance(host, str):
            host = str.encode(host)
        if isinstance(user, str):
            user = str.encode(user)
        if isinstance(password, str):
            password = str.encode(password)
        if isinstance(buffer_path, str):
            buffer_path = str.encode(buffer_path)
        res = api.op2_init(c_int(option), c_char_p(host), c_int(port), c_int(timeout), c_char_p(user),
                           c_char_p(password), c_char_p(buffer_path), c_int(buffer_size))
        return res

    def close(self, op):
        api.op2_close.argtypes = [c_void_p]
        api.op2_close(c_void_p(op))
        return

    def status(self, op):
        api.op2_status.restype = c_int
        api.op2_status.argtypes = [c_void_p]
        res = api.op2_status(c_void_p(op))
        return res

    def get_system_time(self, op, out):
        api.op2_init.restype = c_int
        api.op2_init.argtypes = [c_void_p, POINTER(c_int)]
        res = api.op2_get_system_time(c_void_p(op), byref(out))
        return res

    def try_connect(self, op):
        api.op2_try_connect.restype = c_int
        api.op2_try_connect.argtypes = [c_void_p]
        res = api.op2_try_connect(c_void_p(op))
        return res

    def new_table(self, name):
        if isinstance(name, str):
            name = str.encode(name)
        api.op2_new_table.restype = c_void_p
        api.op2_new_table.argtypes = [c_char_p]
        res = api.op2_new_table(c_char_p(name))
        return res

    def new_row(self, table):
        api.op2_new_row.restype = c_void_p
        api.op2_new_row.argtypes = [c_void_p]
        res = api.op2_new_row(c_void_p(table))
        return res

    def free_table(self, table):
        api.op2_free_table.argtypes = [c_void_p]
        api.op2_free_table(c_void_p(table))
        return

    def add_column(self, table, name, type_, length, mask, defval, expr):
        if isinstance(name, str):
            name = str.encode(name)
        if isinstance(defval, str):
            defval = str.encode(defval)
        if isinstance(expr, str):
            expr = str.encode(expr)
        api.op2_add_column.restype = c_int
        api.op2_add_column.argtypes = [c_void_p, c_char_p, c_int, c_int, c_int, c_char_p, c_char_p]
        res = api.op2_add_column(c_void_p(table), c_char_p(name), c_int(type_), c_int(length), c_int(mask),
                                 c_char_p(defval), c_char_p(expr))
        return res

    def column_count(self, table):
        api.op2_column_count.restype = c_int
        api.op2_column_count.argtypes = [c_void_p]
        res = api.op2_column_count(c_void_p(table))
        return res

    def column_type(self, table, col):
        api.op2_column_type.restype = c_int
        api.op2_column_type.argtypes = [c_void_p, c_int]
        res = api.op2_column_type(c_void_p(table), c_int(col))
        return res

    def value_type(self, table, col):
        api.op2_value_type.restype = c_int
        api.op2_value_type.argtypes = [c_void_p, c_int]
        res = api.op2_value_type(c_void_p(table), c_int(col))
        return res

    def column_name(self, table, col):
        api.op2_column_name.restype = c_char_p
        api.op2_column_name.argtypes = [c_void_p, c_int]
        res = api.op2_column_name(c_void_p(table), c_int(col))
        if isinstance(res, bytes):
            res = bytes.decode(res)
        return res

    def column_index(self, table, name):
        if isinstance(name, str):
            name = str.encode(name)
        api.op2_column_index.restype = c_int
        api.op2_column_index.argtypes = [c_void_p, c_char_p]
        res = api.op2_column_index(c_void_p(table), c_char_p(name))
        return res

    def append_row(self, table):
        api.op2_append_row.restype = c_int
        api.op2_append_row.argtypes = [c_void_p]
        res = api.op2_append_row(c_void_p(table))
        return res

    def new_request(self):
        api.op2_new_request.restype = c_void_p
        res = api.op2_new_request()
        return res

    def free_request(self, r):
        api.op2_free_request.argtypes = [c_void_p]
        res = api.op2_free_request(c_void_p(r))
        return

    def get_stream(self, openplant):
        api.op2_get_stream.restype = c_void_p
        api.op2_get_stream.argtypes = [c_void_p]
        res = api.op2_get_stream(c_void_p(openplant))
        return res

    def set_compress(self, opio, zip_):
        api.op2_set_compress.argtypes = [c_void_p, c_int]
        api.op2_set_compress(c_void_p(opio), c_int(zip_))
        return

    def write_request(self, opio, r):
        api.op2_write_request.restype = c_int
        api.op2_write_request.argtypes = [c_void_p, c_void_p]
        res = api.op2_write_request(c_void_p(opio), c_void_p(r))
        return res

    def write_content(self, opio, t):
        api.op2_write_content.restype = c_int
        api.op2_write_content.argtypes = [c_void_p, c_void_p]
        res = api.op2_write_content(c_void_p(opio), c_void_p(t))
        return res

    def flush_content(self, opio):
        api.op2_flush_content.restype = c_int
        api.op2_flush_content.argtypes = [c_void_p]
        res = api.op2_flush_content(c_void_p(opio))
        return res

    def get_response(self, opio, r):
        api.op2_get_response.restype = c_int
        api.op2_get_response.argtypes = [c_void_p, POINTER(c_void_p)]
        res = api.op2_get_response(c_void_p(opio), byref(r))
        return res

    def has_wall(self, r):
        api.op2_has_wall.restype = c_int
        api.op2_has_wall.argtypes = [c_void_p]
        res = api.op2_has_wall(r)
        return res

    def next_content(self, opio, result, clear, eof):
        api.op2_next_content.restype = c_int
        api.op2_next_content.argtypes = [c_void_p, c_void_p, c_int, POINTER(c_int)]
        res = api.op2_next_content(c_void_p(opio), c_void_p(result), c_int(clear), byref(c_int(eof)))
        return res

    def get_error(self, r):
        api.op2_get_error.restype = c_char_p
        api.op2_get_error.argtypes = [c_void_p]
        res = api.op2_get_error(r)
        return res

    def get_errno(self, r):
        api.op2_get_errno.restype = c_int
        api.op2_get_errno.argtypes = [c_void_p]
        res = api.op2_get_errno(r)
        return res

    def free_response(self, r):
        api.op2_free_response.argtypes = [c_void_p]
        api.op2_free_response(r)
        return

    def set_rowid(self, table, rowid):
        api.op2_set_rowid.restype = c_int
        api.op2_set_rowid.argtypes = [c_void_p, c_int]
        res = api.op2_set_rowid(c_void_p(table), c_int(rowid))
        return res

    def append(self, table, row):
        api.op2_append.restype = c_int
        api.op2_append.argtypes = [c_void_p, c_void_p]
        res = api.op2_append(c_void_p(table), c_void_p(row))
        return res

    def row_count(self, table):
        api.op2_row_count.restype = c_int
        api.op2_row_count.argtypes = [c_void_p]
        res = api.op2_row_count(c_void_p(table))
        return res

    def column_int(self, row, col):
        api.op2_column_int.restype = c_int64
        api.op2_column_int.argtypes = [c_void_p, c_int]
        res = api.op2_column_int(c_void_p(row), c_int(col))
        return res

    def column_double(self, row, col):
        api.op2_column_double.restype = c_double
        api.op2_column_double.argtypes = [c_void_p, c_int]
        res = api.op2_column_double(c_void_p(row), c_int(col))
        return res

    def column_string(self, row, col):
        api.op2_column_string.restype = c_char_p
        api.op2_column_string.argtypes = [c_void_p, c_int]
        res = api.op2_column_string(c_void_p(row), c_int(col))
        return res

    def column_bytes(self, row, col):
        api.op2_column_bytes.restype = c_int
        api.op2_column_bytes.argtypes = [c_void_p, c_int]
        res = api.op2_column_bytes(c_void_p(row), c_int(col))
        return res

    def column_binary(self, row, col):
        api.op2_column_binary.restype = c_void_p
        api.op2_column_binary.argtypes = [c_void_p, c_int]
        res = api.op2_column_binary(c_void_p(row), c_int(col))
        bytesLen = self.column_bytes(row, col)
        values = string_at(res, bytesLen)
        return values

    def column_type(self, row, col):
        api.op2_value_type.restype = c_int
        api.op2_value_type.argtypes = [c_void_p, c_int]
        res = api.op2_value_type(c_void_p(row), c_int(col))
        return res

    def bind_bool(self, row, col, value):
        api.op2_bind_bool.argtypes = [c_void_p, c_int, c_int]
        api.op2_bind_bool(c_void_p(row), c_int(col), c_int(value))
        return

    def bind_int8(self, row, col, value, mask=-1):
        api.op2_bind_int8.argtypes = [c_void_p, c_int, c_int, c_int64]
        api.op2_bind_int8(c_void_p(row), c_int(col), c_int(value), c_int64(mask))
        return

    def bind_int16(self, row, col, value, mask=-1):
        api.op2_bind_int16.argtypes = [c_void_p, c_int, c_int, c_int64]
        api.op2_bind_int16(c_void_p(row), c_int(col), c_int(value), c_int64(mask))
        return

    def bind_int32(self, row, col, value, mask=-1):
        api.op2_bind_int32.argtypes = [c_void_p, c_int, c_int, c_int64]
        api.op2_bind_int32(c_void_p(row), c_int(col), c_int(value), c_int64(mask))
        return

    def bind_int(self, row, col, value, mask=-1):
        api.op2_bind_int.argtypes = [c_void_p, c_int, c_int64, c_int64]
        api.op2_bind_int(c_void_p(row), c_int(col), c_int64(value), c_int64(mask))
        return

    def bind_float(self, row, col, value):
        api.op2_bind_float.argtypes = [c_void_p, c_int, c_float]
        api.op2_bind_float(c_void_p(row), c_int(col), c_float(value))
        return

    def bind_double(self, row, col, value):
        api.op2_bind_double.argtypes = [c_void_p, c_int, c_double]
        api.op2_bind_double(c_void_p(row), c_int(col), c_double(value))
        return

    def bind_string(self, row, col, value):
        if isinstance(value, str):
            value = str.encode(value)
        api.op2_bind_string.argtypes = [c_void_p, c_int, c_char_p]
        api.op2_bind_string(c_void_p(row), c_int(col), c_char_p(value))
        return

    def bind_binary(self, row, col, value, len_):
        api.op2_bind_binary.argtypes = [c_void_p, c_int, c_void_p, c_int]
        api.op2_bind_binary(c_void_p(row), c_int(col), c_void_p(value), c_int(len_))
        return

    def set_table(self, r, t):
        api.op2_set_table.argtypes = [c_void_p, c_void_p]
        api.op2_set_table(c_void_p(r), c_void_p(t))
        return

    def get_table(self, r):
        api.op2_get_table.restype = c_void_p
        api.op2_get_table.argtypes = [c_void_p]
        res = api.op2_get_table(r)
        return res

    def set_option(self, r, key, value):
        if isinstance(key, str):
            key = str.encode(key)
        if isinstance(value, str):
            value = str.encode(value)
        api.op2_set_option.argtypes = [c_void_p, c_char_p, c_char_p]
        api.op2_set_option(c_void_p(r), c_char_p(key), c_char_p(value))
        return

    def get_option(self, r, key, buffer_, len_):
        if isinstance(key, str):
            key = str.encode(key)
        if isinstance(buffer_, str):
            buffer_ = str.encode(buffer_)
        api.op2_get_option.restype = c_char_p
        api.op2_get_option.argtypes = [c_void_p, c_char_p, c_char_p, c_int]
        res = api.op2_get_option(c_void_p(r), c_char_p(key), c_char_p(buffer_), c_int(len_))
        return res

    def set_indices(self, r, name, count, keys):
        if isinstance(name, str):
            name = str.encode(name)
        myArr_int64 = c_int64 * count
        keys_ = myArr_int64()
        for i, v in enumerate(keys):
            keys_[i] = v

        api.op2_set_indices.restype = None
        api.op2_set_indices.argtypes = [c_void_p, c_char_p, c_int, POINTER(c_int64 * count)]
        api.op2_set_indices(c_void_p(r), c_char_p(name), c_int(count), pointer(keys_))
        return

    def set_indices_string(self, r, name, count, keys):
        if isinstance(name, str):
            name = str.encode(name)
        myArr_char_p = c_char_p * count
        keys_ = myArr_char_p()
        for i, v in enumerate(keys):
            keys_[i] = str.encode(v)

        api.op2_set_indices_string.restype = None
        api.op2_set_indices_string.argtypes = [c_void_p, c_char_p, c_int, POINTER(c_char_p * count)]
        api.op2_set_indices_string(c_void_p(r), c_char_p(name), c_int(count), pointer(keys_))
        return

    def add_filter(self, req, l, op, r, rel):
        if isinstance(l, str):
            l = str.encode(l)
        if isinstance(r, str):
            r = str.encode(r)
        api.op2_add_filter.restype = None
        api.op2_add_filter.argtypes = [c_void_p, c_char_p, c_int, c_char_p, c_int]
        api.op2_add_filter(c_void_p(req), c_char_p(l), c_int(op), c_char_p(r), c_int(rel))
        return

    def open_async(self, op, r, cb, owner, error):
        # cbType=CFUNCTYPE(None,c_void_p,c_void_p)
        api.op2_open_async.restype = c_void_p
        # api.op2_open_async.argtypes=[c_void_p,c_void_p,cbType,c_void_p,POINTER(int)]
        res = api.op2_open_async(c_void_p(op), c_void_p(r), cb, c_void_p(owner), byref(error))
        return res

    def close_async(self, ah):
        api.op2_close_async.argtypes = [c_void_p]
        api.op2_close_async(c_void_p(ah))
        return

    def async_subscribe(self, ah, count, ids, onoff):
        myArr_int64 = c_int64 * count
        keys_ = myArr_int64()
        for i, v in enumerate(ids):
            keys_[i] = v
        api.op2_async_subscribe.restype = c_int
        api.op2_async_subscribe.argtypes = [c_void_p, c_int, POINTER(c_int64 * count), c_int]
        res = api.op2_async_subscribe(c_void_p(ah), c_int(count), pointer(keys_), c_int(onoff))
        return res

    def async_subscribe_tags(self, ah, count, tags, onoff):
        myArr_char_p = c_char_p * count
        keys_ = myArr_char_p()
        for i, v in enumerate(tags):
            keys_[i] = str.encode(v)
        api.op2_async_subscribe_tags.restype = c_int
        api.op2_async_subscribe_tags.argtypes = [c_void_p, c_int, POINTER(c_char_p * count), c_int]
        res = api.op2_async_subscribe_tags(c_void_p(ah), c_int(count), pointer(keys_), c_int(onoff))
        return res

    # ***opapi2 start***

    def get_history_byname(self, op, gh, value_types, begin_tm, end_tm, interval, result, errors):
        count = len(value_types)
        myArr_int = c_int * count
        typs_ = myArr_int()
        for i, v in enumerate(value_types):
            typs_[i] = v
        api.op2_get_history_byname.restype = c_int
        api.op2_get_history_byname.argtypes = [c_void_p, c_void_p, POINTER(c_int * count), c_int, c_int, c_int,
                                               POINTER(c_void_p), POINTER(c_int * count)]
        res = api.op2_get_history_byname(c_void_p(op), c_void_p(gh), pointer(typs_), c_int(begin_tm), c_int(end_tm),
                                         c_int(interval), byref(result), byref(errors))
        return res

    # ***opapi2 end***


class Enum(tuple): __getattr__ = tuple.index


Type = Enum(
    ['vtNull', 'vtBool', 'vtInt8', 'vtInt16', 'vtInt32', 'vtInt64', 'vtFloat', 'vtDouble', 'vtDateTime', 'vtString',
     'vtBinary', 'vtObject', 'vtArray', 'vtMap'])


class eType:
    '常用数据类型'
    Null = 0
    Bool = 1
    Int = 5
    Long = 5
    Float = 7
    Datetime = 8
    String = 9
    Binary = 10
    Object = 11


class Connect(IO):
    '连接类'
    actionInsert = 1
    actionDelete = 2
    actionUpdate = 3
    actionSelect = 4

    # option为0,表示自动判断隔离;为1,表示强制隔离
    def __init__(self, ip, port, timeout, user, password, option=0):
        self.op = self.init(option|1024, ip, port, timeout, user, password, None, 0)

    def __del__(self):
        self.close()

    def __useKeyFindType(self, x):
        return {
            'int': eType.Int,
            'text': eType.String,
            'datetime': eType.Datetime,
            'float': eType.Float,
            'double': eType.Float,
            'tinyint': eType.Int,
            'smallint': eType.Int,
            'bigint': eType.Int,
            'blob': eType.Binary
        }.get(x, eType.String)

    def close(self):
        '关闭连接'
        if self.op != None:
            IO.close(self, self.op)
            self.op = None

    def isAlive(self):
        '判断当前连接是否断开'
        return (self.op != None) and (self.status(self.op) == 0)

    def reconnect(self):
        '连接重连'
        return (self.op != None) and (self.try_connect(self.op) == 0)

    def __execute(self, op, request):
        response = c_void_p(None)
        table_get = self.get_table(request)
        opio = self.get_stream(op)
        self.set_compress(opio, 1)
        rv = self.write_request(opio, request)
        if rv == 0:
            if table_get != None:
                rv = self.write_content(opio, table_get)
            rv = self.flush_content(opio)

        if rv != 0:
            raise Exception('write request err')
        rv = self.get_response(opio, response)
        if rv != 0:
            raise Exception('get response err')
        if self.get_errno(response) != 0:
            raise Exception(self.get_error(response))
        resultSet = ResultSet(opio, request, response)
        return resultSet

    def executeQuery(self, sql):
        'SQL请求'
        if self.op == None:
            raise Exception('op_init error')
        request = self.new_request()
        self.set_option(request, 'Reqid', '1')
        self.set_option(request, 'Action', 'ExecSQL')
        self.set_option(request, 'SQL', sql)
        return self.__execute(self.op, request)

    def __modify(self, action, tableName, colNames, rows_):
        rows = None
        if self.op == None:
            raise Exception('op_init error')
        if isinstance(rows_, list):
            rows = numpy.array(rows_)
        elif isinstance(rows_, numpy.ndarray):
            rows = rows_
        else:
            raise Exception('rows type error')

        tableName_getType = ''
        if tableName.find('.') > 0:
            tableName_getType = tableName.partition('.')[2]
        else:
            tableName_getType = tableName

        rowCount = rows.shape[0]
        colCount = rows.shape[1]
        table = self.new_table(tableName)
        try:
            for i in range(colCount):
                cName = colNames[i]
                self.add_column(table, cName, eType.Object, 0, 0, None, None)
        except Exception as e:
            raise Exception(e)

        try:
            for i in range(rowCount):
                row = self.new_row(table)
                for j in range(colCount):
                    v = rows[i][j]
                    if v == None:
                        self.bind_binary(row, j, None, 0)
                    elif isinstance(v, int) or isinstance(v, int):
                        self.bind_int(row, j, v)
                    elif isinstance(v, float):
                        self.bind_double(row, j, v)
                    elif isinstance(v, datetime.datetime):
                        # python only support microsecond
                        self.bind_double(row, j, v.timestamp())
                    else:
                        self.bind_string(row, j, v)
                self.append(table, row)
        except Exception as e:
            raise Exception(e)

        request = self.new_request()
        self.set_table(request, table)
        self.set_option(request, 'Reqid', '1')
        if action == Connect.actionInsert:
            self.set_option(request, 'Action', 'Insert')
        elif action == Connect.actionUpdate:
            self.set_option(request, 'Action', 'Update')

        return self.__execute(self.op, request)

    def __findOrDelete(self, action, tableName, colNames, keys, options):
        tableName_getType = ''
        if tableName.find('.') > 0:
            tableName_getType = tableName.partition('.')[2]
        else:
            tableName_getType = tableName
        table = self.new_table(tableName)
        for v in colNames:
            self.add_column(table, v, eType.Object, 0, 0, None, None)

        if action == Connect.actionDelete:
            try:
                for i, v in enumerate(keys):
                    row = self.new_row(table)
                    if isinstance(v, int) or isinstance(v, int):
                        self.bind_int(row, 0, v)
                    elif isinstance(v, float):
                        self.bind_double(row, 0, v)
                    else:
                        self.bind_string(row, 0, v)
                    self.append(table, row)
            except Exception:
                raise Exception('keys type must consistent')

        request = self.new_request()
        self.set_table(request, table)
        self.set_option(request, 'Reqid', '1')
        if action == Connect.actionSelect:
            self.set_option(request, 'Action', 'Select')
        elif action == Connect.actionDelete:
            self.set_option(request, 'Action', 'Delete')

        if isinstance(options, dict):
            for key, value in list(options.items()):
                if isinstance(value, datetime.datetime):
                    t = value.timetuple()
                    timeStamp = int(time.mktime(t))
                    tStr = str(timeStamp) + str("%.3f" % (float(value.microsecond) / 1000000))[1:]
                else:
                    tStr = str(value)
                self.set_option(request, key, tStr)
        elif options != None:
            raise Exception('options type must dict')

        if len(keys) > 0 and action == Connect.actionSelect:
            if isinstance(keys[0], int):
                self.set_indices(request, None, len(keys), keys)
            elif isinstance(keys[0], str):
                self.set_indices_string(request, None, len(keys), keys)

        return self.__execute(self.op, request)

    def insert(self, tableName, colNames, rows):
        'OPIO插入'
        if (not isinstance(tableName, str)) or (not ((isinstance(colNames, list) or isinstance(colNames, tuple))) or (
                not ((isinstance(rows, list) or isinstance(rows, tuple) or isinstance(rows, numpy.ndarray))))):
            raise Exception('insert parameter error')
        return self.__modify(Connect.actionInsert, tableName, colNames, rows)

    def delete(self, tableName, colNames, keys, options=None):
        'OPIO删除'
        if (not isinstance(tableName, str)) or (not ((isinstance(colNames, list) or isinstance(colNames, tuple))) or (
                not ((isinstance(keys, list) or isinstance(keys, tuple))))):
            raise Exception('delete parameter error')
        return self.__findOrDelete(Connect.actionDelete, tableName, colNames, keys, options)

    def update(self, tableName, colNames, rows):
        'OPIO更新'
        if (not isinstance(tableName, str)) or (not ((isinstance(colNames, list) or isinstance(colNames, tuple))) or (
                not ((isinstance(rows, list) or isinstance(rows, tuple) or isinstance(rows, numpy.ndarray))))):
            raise Exception('update parameter error')
        return self.__modify(Connect.actionUpdate, tableName, colNames, rows)

    def select(self, tableName, colNames, keys, options=None):
        'OPIO查询'
        if (not isinstance(tableName, str)) or (not ((isinstance(colNames, list) or isinstance(colNames, tuple))) or (
                not ((isinstance(keys, list) or isinstance(keys, tuple))))) or (
                (options) and (not isinstance(options, dict))):
            raise Exception('select parameter error')
        return self.__findOrDelete(Connect.actionSelect, tableName, colNames, keys, options)

    def openAsync(self, tableName, cb, keys, snapshot=True):
        '异步订阅'
        async_request = self.new_request()
        table = self.new_table(tableName)
        self.add_column(table, '*', eType.Null, 0, 0, None, None)
        self.set_table(async_request, table)

        keyLen = len(keys)
        if keyLen <= 0:
            raise Exception('keys num is zero')
        if (not isinstance(keys, list)) and (not isinstance(keys, tuple)):
            raise Exception('keys type must be list or tuple')
        if isinstance(keys[0], str):
            self.set_indices_string(async_request, 'GN', keyLen, keys)
        elif isinstance(keys[0], int):
            self.set_indices(async_request, 'ID', keyLen, keys)
        else:
            raise Exception('key must be ID or GN')

        if snapshot:
            self.set_option(async_request, 'Snapshot', '1')
        error = c_int(-1)
        ah = self.open_async(self.op, async_request, cb, None, error)
        if ah == None:
            if async_request != None:
                self.free_request(async_request)
            raise Exception('open_async error:' + str(error.value))
        return Async(ah)


class ResultSet(IO):
    '结果类'

    def __init__(self, opio, request, response):
        self.hasWall = True if self.has_wall(response) == 1 else False
        self.tableResult = self.get_table(response)
        self.request = request
        self.response = response
        self.opio = opio
        self.pos = 0
        self.rowsNum = 0
        self.columnsNum = 0

    def __del__(self):
        self.close()

    def __nextContent(self):
        eof = 0
        if eof == 0:
            rv = self.next_content(self.opio, self.tableResult, 1, eof)
            if rv != 0:
                self.opio = None
            self.pos = 0
            self.rowsNum = self.row_count(self.tableResult)
            self.columnsNum = self.column_count(self.tableResult)
        return eof == 0 and self.pos < self.rowsNum

    def Next(self):
        '下移游标一行'
        if self.hasWall:
            return False
        self.pos += 1
        if self.pos >= self.rowsNum:
            if not self.__nextContent():
                return False
        self.set_rowid(self.tableResult, self.pos)
        return True

    def isHaveWall(self):
        return self.hasWall

    def getString(self, keyOrColumn):
        '根据列或字段获取字符串'
        val = None
        col = 0
        if isinstance(keyOrColumn, str):
            col = self.column_index(self.tableResult, keyOrColumn)
        elif isinstance(keyOrColumn, int):
            col = keyOrColumn
        if col != -1:
            val = self.column_string(self.tableResult, col)
        if isinstance(val, bytes):
            val = bytes.decode(val)
        return val

    def getInt(self, keyOrColumn):
        '根据列或字段获取整型'
        num = None
        col = 0
        if isinstance(keyOrColumn, str):
            col = self.column_index(self.tableResult, keyOrColumn)
        elif isinstance(keyOrColumn, int):
            col = keyOrColumn
        if col != -1:
            num = self.column_int(self.tableResult, col)
        return num

    def getFloat(self, keyOrColumn):
        '根据列或字段获取浮点型'
        num = None
        col = 0
        if isinstance(keyOrColumn, str):
            col = self.column_index(self.tableResult, keyOrColumn)
        elif isinstance(keyOrColumn, int):
            col = keyOrColumn
        if col != -1:
            num = self.column_double(self.tableResult, col)
        return num

    def getDateTime(self, keyOrColumn):
        '根据列或字段获取Datetime结构体'
        date = None
        col = 0
        if isinstance(keyOrColumn, str):
            col = self.column_index(self.tableResult, keyOrColumn)
        elif isinstance(keyOrColumn, int):
            col = keyOrColumn
        if col != -1:
            timestamp = self.column_double(self.tableResult, col)
            date = datetime.datetime.fromtimestamp(timestamp)

        return date

    def getValue(self, keyOrColumn):
        '根据列或字段获取值，返回值类型为实际类型'
        value = None
        col = 0
        if isinstance(keyOrColumn, str):
            col = self.column_index(self.tableResult, keyOrColumn)
        elif isinstance(keyOrColumn, int):
            col = keyOrColumn
        if col != -1:
            type_ = self.value_type(self.tableResult, col)
            if type_ == Type.vtBool or type_ == Type.vtInt8 or type_ == Type.vtInt16 or type_ == Type.vtInt32 or type_ == Type.vtInt64:
                value = self.column_int(self.tableResult, col)
            elif type_ == Type.vtFloat or type_ == Type.vtDouble:
                value = self.column_double(self.tableResult, col)
            elif type_ == Type.vtDateTime:
                v = self.column_double(self.tableResult, col)
                value = datetime.datetime.fromtimestamp(v)
            elif type_ == Type.vtBinary:
                value = self.column_binary(self.tableResult, col)
            else:
                value = self.column_string(self.tableResult, col)
                if isinstance(value, bytes):
                    value = bytes.decode(value)
        return value

    def columnLabel(self, column):
        '根据列获取对应字段名'
        name = None
        if isinstance(column, int):
            name = self.column_name(self.tableResult, column)
        return name

    def columnCount(self):
        '获取列个数'
        return self.column_count(self.tableResult)

    def rowCount(self):
        '获取行个数'
        return self.row_count(self.tableResult)

    def columnType(self, column):
        '获取列类型'
        colType = self.column_type(self.tableResult, column)
        if colType >= 1 and colType < 5:
            return eType.Int
        elif colType == 5:
            return eType.Long
        elif colType >= 6 and colType <= 7:
            return eType.Float
        else:
            return colType

    def valueType(self, column):
        '获取值类型'
        valueType = self.value_type(self.tableResult, column)
        if valueType >= 1 and valueType < 5:
            return eType.Int
        elif valueType == 5:
            return eType.Long
        elif valueType >= 6 and valueType <= 7:
            return eType.Float
        else:
            return valueType

    def close(self):
        '释放内存'
        if self.request != None:
            self.free_request(self.request)
            self.request = None
        if self.response != None:
            self.free_response(self.response)
            self.response = None


class Async(IO):
    '异步订阅类'

    def __init__(self, ah):
        self.ah = ah

    def close(self):
        '关闭订阅'
        if self.ah != None:
            self.close_async(self.ah)
            self.ah = None

    def __subscribe(self, keys, onoff):
        keyLen = len(keys)
        if keyLen <= 0:
            raise Exception('keys num is zero')
        if (not isinstance(keys, list)) and (not isinstance(keys, tuple)):
            raise Exception('keys type must be list or tuple')
        if isinstance(keys[0], str):
            self.async_subscribe_tags(self.ah, keyLen, keys, onoff)
        elif isinstance(keys[0], int):
            self.async_subscribe(self.ah, keyLen, keys, onoff)
        else:
            raise Exception('key must be ID or GN')

    def add(self, keys):
        '动态添加'
        self.__subscribe(keys, 1)

    def remove(self, keys):
        '动态删除'
        self.__subscribe(keys, 0)
