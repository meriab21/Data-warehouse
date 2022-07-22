import copy
import datetime
import json
import os
import random

import re
import time

import requests

from dbstream.tools.parse_data import treat_json_data
from dbstream.tools.regex import replace_query_details
from dbstream.tunnel import create_ssh_tunnel

from google.api_core.exceptions import Forbidden

class DBStream:

    def __init__(self, instance_name, client_id, special_env=None, id_info='dck'):
        self.instance_name = instance_name
        self.instance_type_prefix = ""
        self.ssh_init_port = ""
        self.client_id = client_id
        self.ssh_tunnel = None
        unix_time = time.mktime(datetime.datetime.now().timetuple())
        self.dbstream_instance_id = 'df-' + str(unix_time) + '-' + str(random.randint(1000, 9999))
        self.special_env = special_env if special_env else None
        self.id_info = id_info

    def error_if_function_not_exist(self, function_name):
        raise Exception("Function %s is not defined for %s" % (function_name, type(self).__name__))

    def prefix(self):
        return self.instance_type_prefix + "_" + self.instance_name

    def remote_host(self):
        return os.environ[self.prefix() + "_HOST"]

    def remote_port(self):
        return os.environ[self.prefix() + "_PORT"]

    def credentials(self):
        if self.ssh_tunnel:
            host = self.ssh_tunnel.local_bind_host
            port = self.ssh_tunnel.local_bind_port
        else:
            host = self.remote_host()
            port = self.remote_port()
        return {
            'database': os.environ[self.prefix() + "_DATABASE"],
            'user': os.environ[self.prefix() + "_USERNAME"],
            'host': host,
            'port': port,
            'password': os.environ[self.prefix() + "_PASSWORD"],
        }

    def create_tunnel(self):
        self.ssh_tunnel = create_ssh_tunnel(
            instance=self.instance_name,
            port=self.ssh_init_port,
            remote_host=self.remote_host(),
            remote_port=self.remote_port()
        )
        return self.ssh_tunnel

    def _execute_query_custom(self, query) -> dict:
        self.error_if_function_not_exist("_execute_query_custom")

    def execute_query(self, query, apply_special_env=True):
        query = re.sub(' +', ' ', query)
        query = re.sub(' +\n', '\n', query)
        if apply_special_env and self.special_env:
            query = replace_query_details(query, self.special_env)[0]
        result = self._execute_query_custom(query)
        if isinstance(result, dict):
            if result.get('execute_query'):
                query_create_table = result.get('execute_query').group(0)
                if len(query_create_table.split('.')) > 1:
                    schema_name = query_create_table.split('.')[0]
                    table_name = query_create_table.split('.')[1]
                else:
                    schema_name = None
                    table_name = query_create_table.split('.')[0]

                url = os.environ.get("MONITORING_URL")
                if url:
                    body = {
                        "dbstream_instance_id": self.dbstream_instance_id,
                        "instance_name": self.instance_name,
                        "client_id": self.client_id,
                        "instance_type_prefix": self.instance_type_prefix,
                        "timestamp": str(datetime.datetime.now()),
                        "ssh_tunnel": True if self.ssh_tunnel else False,
                        "local_absolute_path": os.getcwd(),
                        "execute_query": True,
                        'schema_name': schema_name,
                        'table_name': table_name
                    }
                    r = requests.post(url=url, data=json.dumps(body))
                    print(r.status_code)
            return None
        return result

    def _send_data_custom(self, data, replace, **kwargs):
        pass

    def _send(self, **args):
        pass

    def send_data(self, data, replace=True, apply_special_env=True, **kwargs):
        data["columns_name"] = [c.replace(".", "_").replace("(", "_").replace(")", "_") for c in data["columns_name"]]
        data_copy = copy.deepcopy(data)
        if apply_special_env and self.special_env:
            schema_name = data_copy["table_name"].split(".")[0]
            table_name = data_copy["table_name"].split(".")[1]
            data_copy["table_name"] = "%s_%s.%s" % (schema_name, self.special_env, table_name)
            if kwargs.get("other_table_to_update"):
                schema_name = kwargs["other_table_to_update"].split(".")[0]
                table_name = kwargs["other_table_to_update"].split(".")[1]
                kwargs["other_table_to_update"] = "%s_%s.%s" % (schema_name, self.special_env, table_name)

        data_copy2 = copy.deepcopy(data_copy)
        if self._send_data_custom(data_copy, replace, **kwargs) != 0:
            url = os.environ.get("MONITORING_URL")
            if url:
                table_schema_name = data_copy2["table_name"].split(".")
                body = {
                    "dbstream_instance_id": self.dbstream_instance_id,
                    "instance_name": self.instance_name,
                    "client_id": self.client_id,
                    "instance_type_prefix": self.instance_type_prefix,
                    "schema_name": table_schema_name[0],
                    "table_name": table_schema_name[1],
                    "nb_rows": len(data_copy2["rows"]),
                    "nb_columns": len(data_copy2["columns_name"]),
                    "timestamp": str(datetime.datetime.now()),
                    "ssh_tunnel": True if self.ssh_tunnel else False,
                    "local_absolute_path": os.getcwd(),
                    "replace": replace
                }
                r = requests.post(url=url, data=json.dumps(body))
                print(r.status_code)

    def send(self, data, replace=False, apply_special_env=True, delay=5, batch_id=None, **kwargs):
        # data['data'] = generate_dck_info(data['data'])
        list_of_tables_to_send, list_of_pop_fields = treat_json_data(data, batch_id=batch_id, id_info=self.id_info)
        for d in list_of_tables_to_send:
            # if d.get('table_name') == 'test.test_orders_fulfillments_line_items':
            #     print(d.get('data'))
            # else:
            #     print('OK')
            column_names = []
            for r in d['data']:
                for k in r.keys():
                    if k in ('authorization', 'default'):
                        k = k + '_'
                    if k not in column_names:
                        column_names.append(k)
            data_to_send = {
                "columns_name": column_names,
                "rows": [[r.get(c) for c in column_names] for r in d['data']],
                "table_name": d.get('table_name')
            }
            if len(data_to_send['columns_name']) > 0:
                try:
                    self.send_data(data=data_to_send, replace=replace, apply_special_env=apply_special_env, **kwargs)
                except Forbidden:
                    time.sleep(delay)
                    self.send_data(data=data_to_send, replace=replace, apply_special_env=apply_special_env, **kwargs)

    def send_temp_data(self, data, schema_prefix, table, column_names, apply_special_env=True):
        data_to_send = {
            "columns_name": column_names,
            "rows": [[r.get(c) for c in column_names] for r in data],
            "table_name": schema_prefix + '.' + table + '_temp'}
        self.send_data(
            data=data_to_send,
            other_table_to_update=schema_prefix + '.' + table,
            replace=False,
            apply_special_env=apply_special_env)

    def clean(self, selecting_id, schema_prefix, table):
        self.error_if_function_not_exist("clean")

    def send_with_temp_table(self, data, column_names, selecting_id, schema_prefix, table):
        self.send_temp_data(data, schema_prefix, table, column_names)
        self.clean(selecting_id, schema_prefix, table)

    def get_max(self, schema, table, field, filter_clause):
        self.error_if_function_not_exist("get_max")

    def get_data_type(self, table_name, schema_name):
        self.error_if_function_not_exist("get_data_type")

    def create_view_from_columns(self, view_name, columns, schema_name, table_name):
        self.error_if_function_not_exist("create_view_from_columns")

    def create_schema(self, schema_name):
        self.error_if_function_not_exist("create_schema")

    def drop_schema(self, schema_name):
        self.error_if_function_not_exist("drop_schema")

    def build_pydatasource_view(self, query_string):
        self.error_if_function_not_exist("build_pydatasource_view")

    def build_pydatasource_table(self, query_string):
        self.error_if_function_not_exist("build_pydatasource_table")

    def build_pydatasource_table_cascade(self, query_string):
        self.error_if_function_not_exist("build_pydatasource_table_cascade")
