import json
import math

import sqlalchemy
from django.db import models as db_models

from generator import models


class AbstractDatabase:

    def __init__(self):
        self.connection_string = None
        self.type_replacements = models.RESERVED_KEYWORDS
        self.engine = None
        self.tables = None

    def connect_to_database(self, connection_string):
        """
        Used for connecting to database, usually there is no need to override it
        :param connection_string: jdbc connection-like string
        :return: Database object
        """
        self.connection_string = connection_string
        self.engine = sqlalchemy.create_engine(connection_string)
        return self

    def load_tables_info_from_database(self, load_id, job_id):
        loaded_tables = models.TableDumpParams.objects.filter(load_id=load_id, job__id=job_id)
        self.tables = [{"table": table.table_name, "count": table.count, "fields": json.loads(table.fields),
                        "example_data": json.loads(table.example_data)} for table in loaded_tables]
        return self.tables

    def fetch_info_from_table(self, table):
        """

        :param table:
        :return:
        """
        raise NotImplementedError

    def _get_all_tables(self, database_name):
        raise NotImplementedError

    def _find_type(self, type_code):
        raise NotImplementedError

    def save_tables_to_database(self, job, load_id=None):
        if load_id is None:
            load_id = models.TableDumpParams.objects.aggregate(db_models.Max("load_id"))["load_id__max"]
            load_id = 1 if load_id is None else load_id + 1
        for table in self.tables:
            new_table = models.TableDumpParams()
            new_table.job = job
            new_table.load_id = load_id
            new_table.table_name = table["table"]
            new_table.count = table["count"]
            new_table.fields = json.dumps(table["fields"])
            new_table.example_data = json.dumps(table["example_data"])
            new_table.save()
        return load_id

    def _convert_types_to_hive(self, fields):
        raise NotImplementedError

    @staticmethod
    def _get_fields(table):
        fields = table["fields"]
        fields_parsed = []
        for field in fields:
            parsed = field.split(":")
            fields_parsed.append([parsed[0], parsed[1]])
        return fields_parsed

    def generate_create_table(self, table, hive_database_name, table_location, create_table_template, partition,
                              field_template="{} {}"):
        raise NotImplementedError

    def generate_select_string(self, table_part, jira_database_name, partitionize=True):
        raise NotImplementedError

    def _define_partition(self, fields):
        raise NotImplementedError

    def generate_main(self, hive_database_name, source_database_name, load_type, partition_folder, part_name,
                      table_location, create_table_template, create_table_partition, select_string_partition,
                      parameters_template, workflow_subtask_template, partitionize=False):

        raise NotImplementedError

    def generate_properties_and_workflow_for_table(self, partition_folder, part_name, hive_table, source_table,
                                                   select_query, parameters_template, workflow_subtask_template,
                                                   next_item=None):
        props_template = parameters_template
        wf_template = workflow_subtask_template
        tbl_camel = self.to_camel_case(hive_table)
        if next_item is None:
            next_item = "end"
        props = props_template.format(load_folder=partition_folder, part_name=part_name, tbl_camel=tbl_camel,
                                      hive_table=hive_table,
                                      source_table=source_table, select_query=select_query)
        wfs = wf_template.format(upper_table_name=source_table.upper(), tbl_camel=tbl_camel, next=next_item)

        return props, wfs

    def check_name(self, column_name):
        if column_name.upper() in self.type_replacements:
            return "`{}`".format(column_name)
        return column_name

    @staticmethod
    def split(length, parts):
        part_length = math.floor(length / parts)
        queue = []
        position = 0
        for i in range(0, parts):
            new_position = position + part_length
            part = position + new_position
            queue.append(part)
            position = new_position
        queue[-1][1] = length
        return queue

    def get_all_tables_from_source(self, database_name):
        raise NotImplementedError

    @staticmethod
    def to_camel_case(string):
        string = string.replace("_", " ").replace("-", " ")
        return "".join(n for n in string.title() if not n.isspace())
