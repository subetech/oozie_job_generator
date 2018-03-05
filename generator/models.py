from django.db import models

# Create your models here.

AVAILABLE_DATABASES = (
    (1, "PostgreSQL"),
    (2, "MsSQL"),
    (3, "ORACLE"),
    (4, "MySQL"),
    (5, "DB2/AS400")
)

TEMPLATE_TYPES = (
    (1, "create_database"),
    (2, "create_table"),
    (3, "table_location_path"),
    (4, "parameters_part"),
    (5, "workflow_subtask"),
)

RESERVED_KEYWORDS = {'ALL', 'ALTER', 'AND', 'ARRAY', 'AS', 'AUTHORIZATION', 'BETWEEN', 'BIGINT', 'BINARY', 'BOOLEAN',
                     'BOTH', 'BY', 'CASE', 'CAST', 'CHAR', 'COLUMN', 'CONF', 'CREATE', 'CROSS', 'CUBE', 'CURRENT',
                     'CURRENT_DATE', 'CURRENT_TIMESTAMP', 'CURSOR', 'DATABASE', 'DATE', 'DECIMAL', 'DELETE', 'DESCRIBE',
                     'DISTINCT', 'DOUBLE', 'DROP', 'ELSE', 'END', 'EXCHANGE', 'EXISTS', 'EXTENDED', 'EXTERNAL', 'FALSE',
                     'FETCH', 'FLOAT', 'FOLLOWING', 'FOR', 'FROM', 'FULL', 'FUNCTION', 'GRANT', 'GROUP', 'GROUPING',
                     'HAVING', 'IF', 'IMPORT', 'IN', 'INNER', 'INSERT', 'INT', 'INTERSECT', 'INTERVAL', 'INTO', 'IS',
                     'JOIN', 'LATERAL', 'LEFT', 'LESS', 'LIKE', 'LOCAL', 'MACRO', 'MAP', 'MORE', 'NONE', 'NOT', 'NULL',
                     'OF', 'ON', 'OR', 'ORDER', 'OUT', 'OUTER', 'OVER', 'PARTIALSCAN', 'PARTITION', 'PERCENT',
                     'PRECEDING', 'PRESERVE', 'PROCEDURE', 'RANGE', 'READS', 'REDUCE', 'REVOKE', 'RIGHT', 'ROLLUP',
                     'ROW', 'ROWS', 'SELECT', 'SET', 'SMALLINT', 'TABLE', 'TABLESAMPLE', 'THEN', 'TIMESTAMP', 'TO',
                     'TRANSFORM', 'TRIGGER', 'TRUE', 'TRUNCATE', 'UNBOUNDED', 'UNION', 'UNIQUEJOIN', 'UPDATE', 'USER',
                     'USING', 'UTC_TMESTAMP', 'VALUES', 'VARCHAR', 'WHEN', 'WHERE', 'WINDOW', 'WITH', 'COMMIT', 'ONLY',
                     'REGEXP', 'RLIKE', 'ROLLBACK', 'START', 'DAYOFWEEK', 'EXTRACT', 'FLOOR', 'INTEGER', 'PRECISION',
                     'VIEWS', 'TIME', 'NUMERIC'}


class Job(models.Model):
    name = models.CharField(max_length=50)
    db_type = models.IntegerField(choices=AVAILABLE_DATABASES)
    location_path = models.TextField()
    description = models.TextField(blank=True, null=True)
    tables = models.TextField()
    connection_string = models.TextField()


class TypeReplacements(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    type_old = models.CharField(max_length=100)
    type_new = models.CharField(max_length=100)


class TableDumpParams(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    load_id = models.IntegerField()
    table_name = models.CharField(max_length=150)
    count = models.IntegerField(default=0)
    fields = models.TextField()
    example_data = models.TextField()


class Templates(models.Model):
    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    template_type = models.IntegerField(choices=TEMPLATE_TYPES)
    template = models.TextField()
