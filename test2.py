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
                    timestamp1 = resultSet.getDateTime('TM')
                    
                    # 处理 name 和 result
                    name = resultSet.getValue('GN')
                    result = 0 if name and name.strip() else 1
                    
                    # 处理 status
                    status_value = resultSet.getString('DS')
                    status = "192" if status_value == "0" else "-1"
                    
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