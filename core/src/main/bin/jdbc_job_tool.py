#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2018年6月12日

@author: Rassyan
"""

import re
import json
import datetime
import jaydebeapi
from elasticsearch import Elasticsearch

jdbc = {
    "mysql": {
        "driver": "com.mysql.jdbc.Driver",
        "jar": "/opt/datax/plugin/reader/mysqlreader/libs/mysql-connector-java-5.1.34.jar"
    },
    "oracle": {
        "driver": "oracle.jdbc.driver.OracleDriver",
        "jar": "/opt/datax/plugin/reader/oraclereader/libs/commons-collections-3.0.jar"
    },
    "postgresql": {
        "driver": "org.postgresql.Driver",
        "jar": "/opt/datax/plugin/reader/postgresqlreader/libs/postgresql-9.3-1102-jdbc4.jar"
    },
    "sqlserver": {
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "jar": "/opt/datax/plugin/reader/sqlserverreader/libs/sqljdbc4-4.0.jar"
    }
}

if __name__ == '__main__':
    print r'''==================================================='''
    print r'''________          __         ____  ___             '''
    print r'''\______ \ _____ _/  |______  \   \/  /____   ______'''
    print r''' |    |  \\__  \\   __\__  \  \     _/ __ \ /  ___/'''
    print r''' |    `   \/ __ \|  |  / __ \_/     \  ___/ \___ \ '''
    print r'''/_______  (____  |__| (____  /___/\  \___  /____  >'''
    print r'''        \/     \/          \/      \_/   \/     \/ '''
    print r'''==================================================='''
    print r'''                   JDBC Job Tool                   '''
    print r'''==================================================='''
    print("Please make sure in this environment, you can connect to your database via jdbc !")

    prompt = "input environment name (such as dev/online):"
    env = raw_input(prompt)

    prompt = "input the number of database type:\n"
    databases = jdbc.keys()
    i = 1
    for name in databases:
        prompt += "{}. {}\t".format(i, name)
        i += 1
    prompt += "\n"
    while True:
        n = input(prompt)
        database = databases[n - 1]
        if 0 < n <= i:
            jdbc = jdbc[database]
            break

    curs = None
    while True:
        prompt = "jdbc url: "
        url = raw_input(prompt)

        prompt = "input username: "
        username = raw_input(prompt)

        prompt = "input password: "
        password = raw_input(prompt)

        conn = jaydebeapi.connect(jdbc["driver"],
                                  url,
                                  [username, password],
                                  jdbc["jar"])
        curs = conn.cursor()
        if curs:
            break

    columns = []
    mapping = {
        "_doc": {
            "properties": {}
        }
    }
    while True:
        prompt = "input sql (end with ;), better with a limitation (for save query time this time)\nsql: "
        sql = raw_input(prompt)
        while not sql.strip().endswith(";"):
            sql += "\n"
            sql += raw_input()
        sql = sql.strip()[:-1]
        print("================ try to execute sql ================")
        print(sql)
        print("====================================================")
        try:
            curs.execute(sql)
            descs = curs.description
            if re.match(r"^\s*(select|SELECT)\s*\*\s*(from|FROM)\s*", sql):
                sql = re.sub(r"^\s*(select|SELECT)\s*\*\s*(from|FROM)\s*", "select\n          {}\n        from "
                             .format(",\n          ".join([desc[0].encode('utf-8') for desc in descs])), sql)
                print("================== change sql to  ==================")
                print(sql)
                print("====================================================")
        except Exception, err:
            print(err)
            continue

        prompt = "input es _id field (skip if none): "
        _id = raw_input(prompt)

        for desc in descs:
            _name = desc[0].encode('utf-8').lower()
            _type = desc[1]
            if _name == _id:
                columns.append({"name": _name, "type": "id"})
                continue
            elif _type == jaydebeapi.STRING:
                _type = "text"
            elif _type == jaydebeapi.TEXT:
                _type = "text"
            elif _type == jaydebeapi.BINARY:
                _type = "text"
            elif _type == jaydebeapi.NUMBER:
                _type = "long"
            elif _type == jaydebeapi.FLOAT:
                _type = "double"
            elif _type == jaydebeapi.DECIMAL:
                _type = "long"
            elif _type == jaydebeapi.DATE:
                _type = "date"
            elif _type == jaydebeapi.TIME:
                _type = "date"
            elif _type == jaydebeapi.DATETIME:
                _type = "date"
            else:
                print "field {} jdbc type {}, regard as text".format(_name, _type)
                _type = "text"
            columns.append({"name": _name, "type": _type})
            mapping["_doc"]["properties"][_name] = {
                "type": "text",
                "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                }
            } if _type == "text" else {"type": _type}

        curs.close()
        conn.close()
        if columns:
            break

    prompt = "input es index name ('@' can be used only once, meaning {alias}@{partition})\nname: "
    index_name = raw_input(prompt)

    prompt = "input es http port (eg. 127.0.0.1:9200): "
    es_hosts = []
    while True:
        es_host = raw_input(prompt)
        try:
            client = Elasticsearch([es_host], sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)
            es_hosts = [c.host for c in client.transport.connection_pool.connections]
            if es_hosts:
                break
        except Exception as err:
            print err
        print "can not connect es host {}".format(es_host)
    data_nodes = client.cluster.stats().get("nodes", {}).get("count", {}).get("data", 1)

    print("generating {}.py".format(index_name))
    with open("{}.py".format(index_name), 'w') as py_file:
        py_file.write('#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n\n')
        py_file.write('"""\nCreated on {}\n'.format(datetime.datetime.now().strftime("%Y-%m-%d")))
        py_file.write('Generated by DataXes jdbc job tool\n\nhttps://github.com/Rassyan/DataXes\n"""\n\n')
        py_file.write('import sys\nsys.path.append("/opt/datax/bin/")\n')
        py_file.write('import DataXes\n\n\n')
        py_file.write('def jobs(env):\n')
        py_file.write('    jdbc_reader = DataXes.JdbcReader(*{\n')
        py_file.write('        "{}": ("{}reader", "{}", "{}", "{}")\n'.format(env, database, url, username, password))
        py_file.write('    }[env])\n\n')
        py_file.write('    full_data_sqls = [\n        """\n        {}\n        """\n    ]\n\n'.format(sql))
        py_file.write('    eswriter_columns = ')
        py_file.write('\n    '.join(json.dumps(columns, ensure_ascii=False, indent=4).split('\n')))
        py_file.write('\n\n')
        py_file.write('    transformer = []\n\n')
        py_file.write('    def full_data_reader_config(start_time_dt, end_time_dt):\n')
        py_file.write('        return jdbc_reader.reader_config_by_sqls(full_data_sqls)\n\n')
        py_file.write('    return {\n')
        py_file.write('        DataXes.FULL_DATA_JOBS: [\n')
        py_file.write('            (full_data_reader_config, eswriter_columns, "index", transformer)\n')
        py_file.write('        ],\n')
        py_file.write('        DataXes.INCR_DATA_JOBS: []\n')
        py_file.write('    }\n\n\n')
        py_file.write('config = lambda env: {\n')
        py_file.write('    "job": {\n')
        py_file.write('        "process": 4,\n')
        py_file.write('        "datetime_significance": "1s",\n')
        py_file.write('        "datax_args": [\n')
        py_file.write('            "--jvm=-Xms4G -Xmx4G",\n')
        py_file.write('            "-p-Duser.timezone=GMT+8"\n')
        py_file.write('        ]\n')
        py_file.write('    },\n')
        py_file.write('    "es": {\n')
        py_file.write('        "hosts": {\n')
        py_file.write('            "{}": {}\n'.format(env, json.dumps(es_hosts)))
        py_file.write('        }[env],\n')
        py_file.write('        "index_name": "{}",\n'.format(index_name))
        py_file.write('        "action_type": "index",\n')
        py_file.write('        "bulk_size_mb": 10,\n')
        py_file.write('        "bulk_actions": 1000,\n')
        py_file.write('        "max_number_of_retries": 3,\n')
        py_file.write('        "retry_delay_secs": 1\n')
        py_file.write('    },\n')
        py_file.write('    "template": {\n')
        py_file.write('        "mappings": ')
        py_file.write('\n        '.join(json.dumps(mapping, ensure_ascii=False, sort_keys=True, indent=4).split('\n')))
        py_file.write(',\n')
        py_file.write('        "settings": {\n')
        py_file.write('            "index": {\n')
        py_file.write('                "refresh_interval": "1s",\n')
        py_file.write('                "number_of_shards": {{"{}": "{}"}}[env],\n'.format(env, data_nodes))
        py_file.write('                "number_of_replicas": {{"{}": "{}"}}[env]\n'.format(env, 1 % data_nodes))
        py_file.write('            }\n')
        py_file.write('        }\n')
        py_file.write('    }\n')
        py_file.write('}\n\n\n')
        py_file.write('if __name__ == "__main__":\n')
        py_file.write('    DataXes.DataXes(config, jobs, sys.argv[1:])\n')
    print("job script generated!")
