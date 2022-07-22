import pandas as pd

from bigquery.core.Column import detect_type, find_sample_value


def get_table_info(_dbstream, table_and_schema_name):
    split = table_and_schema_name.split(".")
    if len(split) == 2:
        table_name = split[1]
        schema_name = split[0]
    else:
        raise Exception("Invalid table or schema name")
    query = "SELECT column_name, data_type, is_nullable FROM %s.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s'" % (
        schema_name, table_name)
    return _dbstream.execute_query(query)


def format_create_table(_dbstream, data):
    columns_name = data["columns_name"]
    rows = data["rows"]
    params = {}
    df = pd.DataFrame(rows, columns=columns_name)
    df = df.where((pd.notnull(df)), None)
    for i in range(len(columns_name)):
        name = columns_name[i]
        example_max, example_min = find_sample_value(df, name, i)
        col = dict()
        col["example"] = example_max
        type_max = detect_type(_dbstream, name=name, example=example_max, types=data.get("types"))
        if type_max == "TIMESTAMP":
            type_min = detect_type(_dbstream, name=name, example=example_min, types=data.get("types"))
            if type_min == type_max:
                col["type"] = type_max
            else:
                col["type"] = type_min
        else:
            col["type"] = type_max
        params[name] = col

    query = """"""
    query = query + "CREATE TABLE %(table_name)s ("
    col = list(params.keys())
    for i in range(len(col)):
        k = col[i]
        string_example = " --example:" + str(params[k]["example"])[:10].replace("\n", "").replace("%", "") + ''
        if i == len(col) - 1:
            query = query + "\n     `" + k + '` ' + params[k]["type"] + string_example
        else:
            query = query + "\n     `" + k + '` ' + params[k]["type"] + ',' + string_example
    query = query + "\n )"
    print(query)
    return query


def create_table(_dbstream, data, other_table_to_update):
    query = format_create_table(_dbstream, data)
    try:
        _dbstream.execute_query(query % {"table_name": data["table_name"]})
        if other_table_to_update:
            _dbstream.execute_query(query % {"table_name": other_table_to_update})
    except Exception as e:
        if " was not found " in str(e).lower() and " dataset " in str(e).lower():
            schema_name = data['table_name'].split(".")[0]
            _dbstream.create_schema(schema_name)
        else:
            raise e


def create_columns(_dbstream, data, other_table_to_update):
    table_name = data["table_name"]
    rows = data["rows"]
    columns_name = data["columns_name"]
    infos = get_table_info(_dbstream, table_name)
    all_column_in_table = [e['column_name'] for e in infos]
    df = pd.DataFrame(rows, columns=columns_name)
    df = df.where((pd.notnull(df)), None)
    query_table = "alter table %s \n" % table_name
    if other_table_to_update:
        query_other_table = "alter table %s \n" % other_table_to_update
    queries_table = []
    queries_other_table = []
    for column_name in columns_name:
        if column_name not in all_column_in_table:
            example_max, example_min = find_sample_value(df, column_name, columns_name.index(column_name))
            type_max = detect_type(_dbstream, name=column_name, example=example_max, types=data.get("types"))
            if type_max == "TIMESTAMP":
                type_min = detect_type(_dbstream, name=column_name, example=example_min, types=data.get("types"))
                if type_min == type_max:
                    type_ = type_max
                else:
                    type_ = "STRING"
            else:
                type_ = type_max
            query = """
            add COLUMN %s %s
            """ % (column_name, type_)
            queries_table.append(query)
            if other_table_to_update:
                query = """add COLUMN %s %s""" % (column_name, type_)
                queries_other_table.append(query)
    if queries_table:
        query = query_table + ',\n '.join(queries_table) + ";"
        _dbstream.execute_query(query)
    if queries_other_table:
        query = query_other_table + ',\n '.join(queries_other_table) + ";"
        _dbstream.execute_query(query)
    return 0
