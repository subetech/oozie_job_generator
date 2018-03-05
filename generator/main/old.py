import json

import math
import sqlalchemy

connection_string_dev = "postgresql://datalake:c7*gbzyN@s-msk-t-jir-db1.raiffeisen.ru:5432/jiradb"
connection_string_prod = "postgresql://datalake:c7*gbzyN@s-msk-p-jir-db3.raiffeisen.ru:5432/jiradb"

engine = sqlalchemy.create_engine(connection_string_dev)

TYPES_DICT = {'text': "STRING",
              'timestamp': "DATE",
              'numeric': "DECIMAL",
              'int8': "INT",
              'int4': "INT",
              'varchar': "STRING",
              'float8': "FLOAT",
              'timestamptz': "DATE",
              'bpchar': "STRING",
              'bytea': "BINARY",
              'bool': "BOOLEAN"}

RESERVED_DICT = {'ALL', 'ALTER', 'AND', 'ARRAY', 'AS', 'AUTHORIZATION', 'BETWEEN', 'BIGINT', 'BINARY', 'BOOLEAN',
                 'BOTH', 'BY', 'CASE', 'CAST', 'CHAR', 'COLUMN', 'CONF', 'CREATE', 'CROSS', 'CUBE', 'CURRENT',
                 'CURRENT_DATE', 'CURRENT_TIMESTAMP', 'CURSOR', 'DATABASE', 'DATE', 'DECIMAL', 'DELETE', 'DESCRIBE',
                 'DISTINCT', 'DOUBLE', 'DROP', 'ELSE', 'END', 'EXCHANGE', 'EXISTS', 'EXTENDED', 'EXTERNAL', 'FALSE',
                 'FETCH', 'FLOAT', 'FOLLOWING', 'FOR', 'FROM', 'FULL', 'FUNCTION', 'GRANT', 'GROUP', 'GROUPING',
                 'HAVING', 'IF', 'IMPORT', 'IN', 'INNER', 'INSERT', 'INT', 'INTERSECT', 'INTERVAL', 'INTO', 'IS',
                 'JOIN', 'LATERAL', 'LEFT', 'LESS', 'LIKE', 'LOCAL', 'MACRO', 'MAP', 'MORE', 'NONE', 'NOT', 'NULL',
                 'OF', 'ON', 'OR', 'ORDER', 'OUT', 'OUTER', 'OVER', 'PARTIALSCAN', 'PARTITION', 'PERCENT', 'PRECEDING',
                 'PRESERVE', 'PROCEDURE', 'RANGE', 'READS', 'REDUCE', 'REVOKE', 'RIGHT', 'ROLLUP', 'ROW', 'ROWS',
                 'SELECT', 'SET', 'SMALLINT', 'TABLE', 'TABLESAMPLE', 'THEN', 'TIMESTAMP', 'TO', 'TRANSFORM', 'TRIGGER',
                 'TRUE', 'TRUNCATE', 'UNBOUNDED', 'UNION', 'UNIQUEJOIN', 'UPDATE', 'USER', 'USING', 'UTC_TMESTAMP',
                 'VALUES', 'VARCHAR', 'WHEN', 'WHERE', 'WINDOW', 'WITH', 'COMMIT', 'ONLY', 'REGEXP', 'RLIKE',
                 'ROLLBACK', 'START', 'CACHE', 'CONSTRAINT', 'FOREIGN', 'PRIMARY', 'REFERENCES', 'DAYOFWEEK', 'EXTRACT',
                 'FLOOR', 'INTEGER', 'PRECISION', 'VIEWS', 'TIME', 'NUMERIC'}

TABLE_LOCATION_PATH = "/datalake/data/raw/jira/main/{}"


def get_template(template_file):
    with open("templates/{}".format(template_file)) as template:
        return template.read()


def find_type(type_code):
    selection = engine.execute("select typname from pg_type where oid = {} order by oid".format(type_code))
    return selection.fetchone()[0]


def check_name(column_name):
    if column_name.upper() in RESERVED_DICT:
        return "`{}`".format(column_name)
    return column_name


def fetch_info_from_table(table):
    try:
        selection = engine.execute("SELECT t.* FROM {} t limit 5".format(table))
        description = selection._cursor_description()
        fields = []
        for col in description:
            fields.append("{}:{}".format(col[0], find_type(col[1])))
        some_data = []
        for part in selection:
            some_data.append(part)
        return fields, some_data
    except sqlalchemy.exc.ProgrammingError:
        print("NOTHING FOUND FOR {}".format(table))
    print("\n")


def get_all_tables():
    selection = engine.execute("SELECT TABLE_NAME FROM information_schema.tables where table_schema = 'jira'")
    tables = []
    for tab in selection:
        tables.append(tab[0])
    return tables


def filter_tables(tables):
    tables = list(filter(lambda x: x[0:3] != 'AO_', tables))
    new_tables = []
    for tab in tables:
        selection = engine.execute("SELECT count(*) from jira.{}".format(tab))
        fetchone_ = selection.fetchone()[0]
        if fetchone_ != 0:
            fields, some_data = fetch_info_from_table(tab)
            new_tables.append(
                {"table": tab, "count": fetchone_, "fields": fields, "example_data": [str(x) for x in some_data]})
    return new_tables


def write_json(data):
    with open("tables.json", "w") as textfile:
        textfile.write(json.dumps(data, indent=4, ensure_ascii=False))


def get_fields(part):
    fields = part["fields"]
    fields_parsed = []
    for field in fields:
        parsed = field.split(":")
        fields_parsed.append([parsed[0], parsed[1]])
    return fields_parsed


def convert_type_to_hive(old_type):
    return TYPES_DICT[old_type]


def convert_types_to_hive(parsed_fields):
    new_fields = []
    for parsed_field in parsed_fields:
        new_fields.append([check_name(parsed_field[0]), convert_type_to_hive(parsed_field[1])])
    return new_fields


def generate_fields_string(fields, template="{} {}"):
    strs = []
    for field in fields:
        strs.append(template.format(field[0], field[1]))
    return ",\n\t".join(strs)


def crt_table(table_part, hive_database_name, partition="PARTITIONED BY (dlk_load_date STRING)"):
    table_name = "{}.{}".format(hive_database_name, table_part["table"])
    fields = generate_fields_string(convert_types_to_hive(get_fields(table_part)))
    location = TABLE_LOCATION_PATH.format(table_part["table"])
    table_string = get_template("create_table.tpl").format(table_name=table_name, fields=fields, partition=partition,
                                                           location=location)
    return table_string


def find_in_list(some_list, keyword):
    for l in some_list:
        if l.find(keyword) != -1:
            return l
    return None


def define_partitions(fields):
    all_fields = [x[0] for x in fields]
    in_list = find_in_list(all_fields, "updated")
    if in_list is not None:
        return in_list
    in_list = find_in_list(all_fields, "created")
    if in_list is not None:
        return in_list
    in_list = find_in_list(all_fields, "timestamp")
    if in_list is not None:
        return in_list
    return ""


def select_string(table_part, jira_database_name, partitionize=True):
    partition = "{jira_partition_col}::date = date '${{loadFolder}}' and"
    select_str = "select {fields}, text('${{loadFolder}}') as ${{partName}} from {jira_table} t where " \
                 "{partitions} $CONDITIONS"
    jira_table = "{}.{}".format(jira_database_name, table_part["table"])
    all_fields = convert_types_to_hive(get_fields(table_part))
    new_fields = []
    found_partition = define_partitions(all_fields)
    for f in all_fields:
        new_fields.append("t.{}".format(f[0]))
    fields = ", ".join(new_fields)
    if found_partition != "" and partitionize:
        partition = partition.format(jira_partition_col=found_partition)
    else:
        partition = ""
    return select_str.format(fields=fields, jira_table=jira_table, partitions=partition)


def to_camel_case(string):
    string = string.replace("_", " ").replace("-", " ")
    return "".join(n for n in string.title() if not n.isspace())


def generate_part_for_properties_and_workflow(load_folder, part_name, hive_table, source_table, select_query,
                                              next_item=None):
    props_template = get_template("param_part.tpl")
    wf_template = get_template("workflow_subtask.tpl")
    tbl_camel = to_camel_case(hive_table)
    if next_item is None:
        next_item = "end"
    props = props_template.format(load_folder=load_folder, part_name=part_name, tbl_camel=tbl_camel,
                                  hive_table=hive_table,
                                  source_table=source_table, select_query=select_query)
    wfs = wf_template.format(upper_table_name=source_table.upper(), tbl_camel=tbl_camel, next=next_item)

    return props, wfs


def save_to_disc(some_file, string):
    with open("saved/{}".format(some_file), "w") as fl:
        fl.write(string)


def generate_workflow(tables, hive_database_name, jira_database_name, load_type, cob_folder, part_name,
                      partitionize=True):
    """
    Формирует основные настройки и SQL запросы
    :param tables: словарь таблиц, полученный, например из сохраненного JSON
    :param hive_database_name: Название базы данных в hive, например raw_jira_downloaded
    :param jira_database_name: Название базы данных в источнике
    :param load_type: тип загрузки для названия тасков в workflow, например load_orc
    :param cob_folder: название партиции, например 2013-04-09
    :param part_name: названия папки для партиции, например "dlk_cob_date"
    :param partitionize: если True, то будут сгенерированы параметры
                            для периодической загрузки, если False - для разовой
    :return:
    """
    first_part = "loadFolder={load_folder} \npartName={part_name}".format(load_folder=cob_folder, part_name=part_name)
    create_tables = []
    properties = []
    workflows = []
    for i in range(0, len(tables)):
        create_tables.append(crt_table(tables[i], hive_database_name))
        select_str = select_string(tables[i], jira_database_name, partitionize)
        if i < len(tables) - 1:
            next_item = "{load_type}_{upper_table_name}".format(load_type=load_type,
                                                                upper_table_name=tables[i + 1]["table"].upper())
        else:
            next_item = None
        props, wf = generate_part_for_properties_and_workflow(cob_folder, part_name, tables[i]["table"],
                                                              tables[i]["table"],
                                                              select_str, next_item)
        properties.append(props)
        workflows.append(wf)
    return create_tables, properties, workflows


def split(length, parts):
    length = length
    part_length = math.floor(length / parts)
    queue = []
    position = 0
    for j in range(0, parts):
        new_position = position + part_length
        part = [position, new_position]
        queue.append(part)
        position = new_position
    queue[-1][1] = length
    return queue


def save(create_tables, properties, workflows, parts=1):
    save_to_disc("crt_tbls.sql", "\n".join(create_tables))
    queue = split(len(workflows), parts)
    for idx, part in enumerate(queue):
        save_to_disc("properties_{}.txt".format(idx), "\n\n".join(properties[part[0]:part[1]]))
        save_to_disc("workflows_{}.txt".format(idx), "\n\n".join(workflows[part[0]:part[1]]))


if __name__ == '__main__':
    with open("tables.json", "r") as tabls:
        datas = tabls.read()
        jss = json.loads(datas)
    sql_tabs, wf_props, wf_workflows = generate_workflow(jss, "raw_jira_main", "jira", "load_orc", "2017-02-26",
                                                         "dlk_cob_date", False)
    save(sql_tabs, wf_props, wf_workflows, 2)
