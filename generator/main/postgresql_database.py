from generator.main.abstract_database import AbstractDatabase

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


class PostgresqlDatabase(AbstractDatabase):

    def generate_create_table(self, table, hive_database_name, table_location, create_table_template, partition,
                              field_template="{} {}"):

        def generate_fields_string(hive_fields):
            strs = []
            for field in hive_fields:
                strs.append(field_template.format(field[0], field[1]))
            return ",\n\t".join(strs)

        table_name = "{}.{}".format(hive_database_name, table["table"])
        fields = generate_fields_string(self._convert_types_to_hive(self._get_fields(table)))
        location = table_location.format(table["table"])
        table_string = create_table_template.format(table_name=table_name, fields=fields,
                                                    partition=partition,
                                                    location=location)
        return table_string

    def generate_select_string(self, table_part, jira_database_name,
                               partition="{jira_partition_col}::date = date '${{loadFolder}}' and", partitionize=True):

        select_str = "SELECT {fields}, TEXT('${{loadFolder}}') AS ${{partName}} FROM {jira_table} t WHERE " \
                     "{partitions} $CONDITIONS"
        jira_table = "{}.{}".format(jira_database_name, table_part["table"])
        all_fields = self._convert_types_to_hive(self._get_fields(table_part))
        new_fields = []
        found_partition = self._define_partition(all_fields)
        for f in all_fields:
            new_fields.append("t.{}".format(f[0]))
        fields = ", ".join(new_fields)
        if found_partition != "" and partitionize:
            partition = partition.format(jira_partition_col=found_partition)
        else:
            partition = ""
        return select_str.format(fields=fields, jira_table=jira_table, partitions=partition)

    def generate_main(self, hive_database_name, source_database_name, load_type, partition_folder, part_name,
                      table_location, create_table_template, create_table_partition, select_string_partition,
                      parameters_template, workflow_subtask_template, partitionize=False):

        first_part = "loadFolder={load_folder} \npartName={part_name}".format(load_folder=partition_folder,
                                                                              part_name=part_name)
        create_tables = []
        properties = []
        workflows = []
        for i in range(0, len(self.tables)):
            create_tables.append(
                self.generate_create_table(self.tables[i], hive_database_name, table_location, create_table_template,
                                           create_table_partition))
            select_str = self.generate_select_string(self.tables[i], source_database_name, select_string_partition,
                                                     partitionize)
            if i < len(self.tables) - 1:
                next_item = "{load_type}_{upper_table_name}".format(load_type=load_type,
                                                                    upper_table_name=self.tables[i + 1][
                                                                        "table"].upper())
            else:
                next_item = None
            props, wf = self.generate_properties_and_workflow_for_table(partition_folder, part_name,
                                                                        self.tables[i]["table"],
                                                                        self.tables[i]["table"],
                                                                        select_str, parameters_template,
                                                                        workflow_subtask_template, next_item)
            properties.append(props)
            workflows.append(wf)
        return create_tables, properties, workflows

    def get_all_tables_from_source(self, table_schema):
        tables = self._get_all_tables(table_schema)
        new_tables = []
        for tab in tables:
            table_count = self.engine.execute("SELECT COUNT(*) FROM {}.{}".format(table_schema, tab)).fetchone()[0]
            fields, some_data = self.fetch_info_from_table(tab)
            new_tables.append(
                {"table": tab, "count": table_count, "fields": fields, "example_data": [str(x) for x in some_data]}
            )
        self.tables = new_tables
        return new_tables

    def _get_all_tables(self, table_schema):
        selection = self.engine.execute(
            "SELECT TABLE_NAME FROM information_schema.tables where table_schema = '{}'".format(table_schema))
        tables = []
        for tab in selection:
            tables.append(tab[0])
        return tables

    def fetch_info_from_table(self, table):
        selection = self.engine.execute("SELECT t.* FROM {} t limit 5".format(table))
        description = selection._cursor_description()
        fields = []
        for col in description:
            fields.append("{}:{}".format(col[0], self._find_type(col[1])))
        some_data = []
        for part in selection:
            some_data.append(part)
        return fields, some_data

    def _convert_types_to_hive(self, postgres_fields):

        def convert_type_to_hive(old_type):
            return TYPES_DICT[old_type]

        new_fields = []
        for postgres_field in postgres_fields:
            new_fields.append([self.check_name(postgres_field[0]), convert_type_to_hive(postgres_field[1])])
        return new_fields

    def _define_partition(self, fields):

        def find_in_list(some_list, keyword):
            for l in some_list:
                if l.find(keyword) != -1:
                    return l
            return None

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

    def _find_type(self, type_code):
        selection = self.engine.execute("select typname from pg_type where oid = {} order by oid".format(type_code))
        return selection.fetchone()[0]
