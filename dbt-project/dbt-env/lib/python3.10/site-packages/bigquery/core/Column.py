import copy
import pandas as pd

from bigquery.core.mapping_type import mapping_types


def get_columns_type(_dbstream, schema_name, table_name):
    d = {}
    r = _dbstream.get_data_type(table_name, schema_name)
    for i in r:
        d[i["column_name"]] = i["data_type"]
    return d


def change_type(_dbstream, table_name, column_name, type):
    query = """
    CREATE OR REPLACE TABLE %(table_name)s AS 
    SELECT * EXCEPT (%(column_name)s), CAST(%(column_name)s AS %(type)s) AS %(column_name)s FROM %(table_name)s;
    """ % {
        "table_name": table_name,
        "column_name": column_name,
        "type": type
    }
    _dbstream.execute_query(query)
    return query


def bool_to_str(_dbstream, table_name, column_name):
    query = """
        CREATE OR REPLACE TABLE %(table_name)s AS 
        SELECT * EXCEPT (%(column_name)s), CAST(%(column_name)s AS STRING) AS %(column_name)s FROM %(table_name)s;
        """ % {
        "table_name": table_name,
        "column_name": column_name,
    }
    _dbstream.execute_query(query)
    return query


def change_columns_type(_dbstream, data, other_table_to_update):
    table_name = data["table_name"].split('.')
    columns_type = get_columns_type(_dbstream, table_name=table_name[1], schema_name=table_name[0])
    rows = data["rows"]
    columns_name = data["columns_name"]
    df = pd.DataFrame(rows, columns=columns_name)

    for c in columns_name:
        example = find_sample_value(df, c, columns_name.index(c))[0]
        if isinstance(example, float):
            if columns_type.get(c) != "FLOAT64" and columns_type.get(c) != "STRING":
                change_type(_dbstream, table_name=data["table_name"], column_name=c, type="FLOAT64")
                if other_table_to_update:
                    change_type(_dbstream, table_name=other_table_to_update, column_name=c, type="FLOAT64")
        if isinstance(example, str):
            if columns_type.get(c) != "STRING" and columns_type.get(c) != "BOOL" and columns_type.get(c) != "TIMESTAMP":
                change_type(_dbstream, table_name=data["table_name"], column_name=c, type="STRING")
                if other_table_to_update:
                    change_type(_dbstream, table_name=other_table_to_update, column_name=c, type="STRING")


def columns_type_bool_to_str(_dbstream, data, other_table_to_update):
    table_name = data["table_name"].split('.')
    columns_type = get_columns_type(_dbstream, table_name=table_name[1], schema_name=table_name[0])
    rows = data["rows"]
    columns_name = data["columns_name"]
    df = pd.DataFrame(rows, columns=columns_name)

    for c in columns_name:
        example = find_sample_value(df, c, columns_name.index(c))[0]
        if isinstance(example, str):
            if columns_type.get(c) == "BOOL":
                bool_to_str(_dbstream, table_name=data["table_name"], column_name=c)
                if other_table_to_update:
                    bool_to_str(_dbstream, table_name=other_table_to_update, column_name=c)


def detect_type(_dbstream, name, example, types):
    print('Define type of %s...' % name)
    if types and types.get(name):
        try:
            return mapping_types[types.get(name)]
        except KeyError:
            raise Exception(
                "Type %s missing in mapping_types dictionary [dbstream types] <> [big query types]" % types.get(name)
            )
    try:
        query = "SELECT CAST('%s' as DATE)" % example
        _dbstream.execute_query(query)
        return "DATE"
    except:
        try:
            query = "SELECT CAST('%s' as TIMESTAMP)" % example
            _dbstream.execute_query(query)
            return "TIMESTAMP"
        except:
            pass
    if isinstance(example, bool):
        return "BOOL"
    elif isinstance(example, int):
        return "INT64"
    elif isinstance(example, float):
        return "FLOAT64"
    else:
        return "STRING"


def convert_to_bool(x):
    if x.lower() == "true" or x == 1 or x.lower() == "t":
        return True
    if x.lower() == "false" or x == 0 or x.lower() == "f":
        return False
    else:
        raise Exception


def convert_to_int(x):
    if x[-2:] == ".0":
        return int(x.replace(".0", ""))
    else:
        return int(x)


def len_or_max(s):
    if isinstance(s, str):
        return len(s)
    return s


def find_sample_value(df, name, i):
    df1 = df[name].dropna()
    try:
        df1 = df1.apply(lambda x: str(x))
    except:
        pass
    try:
        df1 = df1.apply(lambda x: convert_to_bool(x))
    except:
        try:
            df1 = df1.apply(lambda x: convert_to_int(x))
        except:
            try:
                df1 = df1.apply(lambda x: float(x))
            except:
                pass
    df1_copy = copy.deepcopy(df1)
    if df1.dtype == 'object':
        df1 = df1.apply(lambda x: (str(x.encode()) if isinstance(x, str) else x) if x is not None else '')
        if df1.empty:
            return None, None
        else:
            return df1_copy[df1.map(len_or_max) == df1.map(len_or_max).max()].iloc[0], \
                   df1_copy[df1.map(len_or_max) == df1.map(len_or_max).min()].iloc[0]
    elif df1.dtype == 'int64':
        max = int(df1.max())
        min = int(df1.min())
        return max, min
    elif df1.dtype == 'float64':
        max = float(df1.max())
        min = float(df1.min())
        return max, min
    else:
        rows = df.values.tolist()
        for row in rows:
            if row[i] is not None:
                return row[i], row[i]
        return None, None
