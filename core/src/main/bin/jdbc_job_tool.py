#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 2018年6月12日

@author: Rassyan
"""

import jaydebeapi
import json
import datetime

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
            "properties": {},
            "dynamic_templates": [
                {
                    "strings": {
                        "match_mapping_type": "string",
                        "mapping": {
                            "type": "keyword"
                        }
                    }
                }
            ]
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
                print _name, _type
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
        if es_host:
            es_hosts.append(es_host)
        elif es_hosts:
            prompt = "input es host: "
            break
        prompt = "input other es http port (skip if none): "

    print("generating {}.py".format(index_name))
    with open("{}.py".format(index_name), 'w') as py_file:
        py_file.write("#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n\n")
        py_file.write('''"""\nCreated on {}\n'''.format(datetime.datetime.now().strftime("%Y-%m-%d")))
        py_file.write('''Generated by DataXes jdbc job tool\n\nhttps://github.com/Rassyan/DataXes\n"""\n\n''')
        py_file.write("import sys\nsys.path.append('/opt/datax/bin/')\n")
        py_file.write("from DataXes import *\n\n")
        py_file.write('''dataxes = DataXes("{}.yml")\n'''.format(index_name))
        py_file.write('''jdbc_reader = JdbcReader("{}reader", "{}", "{}", "{}")\n\n'''.format(
            database, url, username, password))
        py_file.write("full_data_sqls = [\n    '''\n{}\n    '''\n]\n\n".format(sql))
        py_file.write("eswriter_columns = {}\n\n".format(json.dumps(columns, ensure_ascii=False, indent=4)))
        py_file.write("transformer = []\n\n\n")
        py_file.write("def full_data_reader_config(start_time_dt, end_time_dt):\n")
        py_file.write("    return jdbc_reader.reader_config_by_sqls(full_data_sqls)\n\n\n")
        py_file.write("jobs = {\n    FULL_DATA_JOBS: [\n")
        py_file.write("        (full_data_reader_config, eswriter_columns, 'index', transformer)\n")
        py_file.write("    ]\n}\n\n")
        py_file.write("if __name__ == '__main__':\n")
        py_file.write("    if len(sys.argv) > 1:\n")
        py_file.write("        if sys.argv[1] == '-ff' or sys.argv[1] == '--forcefull':\n")
        py_file.write("            # 强制跑全量切换\n")
        py_file.write("            dataxes.do_jobs(jobs, force_full=True)\n")
        py_file.write("        elif sys.argv[1] == '-rb' or sys.argv[1] == '--rollback':\n")
        py_file.write("            # 索引版本回滚\n")
        py_file.write("            dataxes.rollback()\n")
        py_file.write("        elif sys.argv[1] == '-rf' or sys.argv[1] == '--rollforward':\n")
        py_file.write("            # 索引版本前滚\n")
        py_file.write("            dataxes.rollforward()\n")
        py_file.write("        else:\n")
        py_file.write("            print 'invalid arguments.'\n")
        py_file.write("    else:\n")
        py_file.write("        # 自动判断全量/增量同步\n")
        py_file.write("        dataxes.do_jobs(jobs)\n")

    print("generating {}.yml".format(index_name))
    with open("{}.yml".format(index_name), 'w') as yml_file:
        yml_file.write("job:\n  process: 4\n  ")
        yml_file.write("datetime_significance: 1s # 以此基数值对当前系统时间来向下取整，")
        yml_file.write("如2h的配置会将 2019-01-02 11:22:33 取整得到 2019-01-02 10:00:00 作为本次作业offset的结束时间\n")
        yml_file.write("  datax_args:\n  - '--jvm=-Xms16G -Xmx16G'\n  - '-p-Duser.timezone=GMT+8'\n\n")
        yml_file.write("es:\n  hosts:\n")
        for host in es_hosts:
            yml_file.write("  - {}\n".format(host))
        yml_file.write("  index_name: {}\n".format(index_name))
        yml_file.write("  template_file: {}_template.json\n".format(index_name))
        yml_file.write("  action_type: index\n")
        yml_file.write("  bulk_actions: 1000\n")
        yml_file.write("  bulk_size_mb: 20\n")
        yml_file.write("  retry_delay_secs: 1\n")
        yml_file.write("  max_number_of_retries: 3\n")

    print("generating {}_template.json".format(index_name))
    with open("{}_template.json".format(index_name), 'w') as json_file:
        json_file.write(json.dumps({"settings": {}, "mappings": mapping}, ensure_ascii=False, indent=2))

    print("job script generated!")
