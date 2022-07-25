import collections
import copy
import datetime
import json


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def treat_data(raw_data, column_names):
    rows = []
    for data in raw_data:
        flatten_data = flatten(data)
        row = []
        for column in column_names:
            try:
                if isinstance(flatten_data[column], list):
                    row.append(json.dumps(flatten_data[column]))
                else:
                    try:
                        d = datetime.datetime.strptime(flatten_data[column], '%d/%m/%Y')
                        row.append(d.strftime("%Y-%m-%d"))
                    except:
                        row.append(flatten_data[column])
            except KeyError:
                row.append(None)
        rows.append(row)
    return rows


def treat_dict_and_unnest(list_of_dict, columns_to_remove=[], columns_to_unnest=None):
    result = []
    for d in list_of_dict:
        for k in list(d.keys()):
            if k in columns_to_remove:
                del d[k]
        new_d = flatten(d)
        if columns_to_unnest:
            field_to_unnest = new_d.get(columns_to_unnest) if new_d.get(columns_to_unnest) else []
            for l in field_to_unnest:
                new_sub_d = copy.deepcopy(new_d)
                new_sub_d.update(flatten({columns_to_unnest: l}))
                del new_sub_d[columns_to_unnest]
                result.append(new_sub_d)
        else:
            result.append(new_d)
    for r in result:
        for k in r.keys():
            if isinstance(r[k], list) or isinstance(r[k], dict):
                r[k] = json.dumps(r[k])
            elif r[k] == "":
                r[k] = None
    return result
