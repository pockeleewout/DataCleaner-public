import typing
import os
import re

import sqlalchemy
import pandas
import psycopg2

from .db_object import db, table_names
from .exceptions import *

if typing.TYPE_CHECKING:
    from .data_version import DataVersion


def delete_table(table_id: int) -> bool:
    """
    Delete the table in the database
    :param table_id: ID of the table to delete
    :return: Success status
    """
    table: DataTable = DataTable.query.get(table_id)

    if table:
        # Clear the data
        table.clear()
        # Delete
        db.session.delete(table)
        db.session.commit()
        return True
    else:
        return False


def join_tables(table_ids: list, *args) -> sqlalchemy.sql.expression.FromClause:
    """"""
    from .data_column import DataColumn

    tables: list = [DataTable.query.get(table_id) for table_id in table_ids]

    if None in tables:
        # If there are invalid ids for tables, fail
        raise JoinError()

    # Check if the column ids are valid
    for l in args:
        # If the argument is not a list, fail
        if not isinstance(l, list):
            raise JoinError("Non-list argument for join_tables")
        # If the length of the list doesn't match, fail
        if len(l) != len(table_ids):
            raise JoinError("Columns to join do not match amount of tables")
        # Check if the columns actually belong in the right tables
        for i in range(len(l)):
            if tables[i].columns.filter(DataColumn.id == l[i]).count() <= 0 and l[i] is not None:
                raise JoinError("Column to match is not part of the table being joined")

    # Start the join clause with the first table
    join_clause = tables[0].sql_table_clause()

    # Generate the join statement
    for table_i in range(len(table_ids) - 1):
        table1: DataTable = tables[table_i]
        table2: DataTable = tables[(table_i + 1) % len(table_ids)]

        clauses = []
        for and_i in range(len(args)):
            # Check if both are valid, else there is a None so we don't join on this
            if args[and_i][table_i] and args[and_i][(table_i + 1) % len(table_ids)]:
                column1_clause = table1.sql_table_clause().columns[
                    str(args[and_i][table_i])
                ]
                column2_clause = table2.sql_table_clause().columns[
                    str(args[and_i][(table_i + 1) % len(table_ids)])
                ]
                clauses.append(column1_clause == column2_clause)
                # print("success")
            else:
                # print("fail")
                pass

        if len(clauses) > 0:
            and_clause = sqlalchemy.sql.expression.and_(*clauses)

            # Generate a full outer join with the 2
            join_clause = join_clause.join(table2.sql_table_clause(), and_clause, full=True)

        else:
            join_clause = join_clause.join(table2.sql_table_clause(), db.text("1=1"), full=True)

    return join_clause


def _match_sql_types(t: str) -> str:
    re_smallint = re.compile("(?i)(?:small|tiny)int(?:\s?\(\d+?\))?|byte")
    re_integer = re.compile("(?i)(?:medium)?int(?:eger)?(?:\s?\(\d+?\))?")
    re_bigint = re.compile("(?i)bigint(?:\s\(\d+?\))?")
    re_decimal = re.compile("(?i)(?:decimal|numeric)(?:\s?\((\d+?)(?:\s?,\s?(\d+?))?\))?")
    re_real = re.compile("(?i)real")
    re_double = re.compile("(?i)(?:float\(\d+?\)|double(?:\sprecision)?(?:\s?\((\d+?)\)(?:\s?,\s?(\d+?))?)?)")
    re_smallserial = re.compile("(?i)smallserial")
    re_serial = re.compile("(?i)serial")
    re_bigserial = re.compile("(?i)bigserial")
    re_money = re.compile("(?i)(?:small)?money")
    re_varchar = re.compile("(?i)(?:varchar|character varying)(?:\s?\(\s?(\d+)\s?\))?")
    re_char = re.compile("(?i)char(?:acter)?(?:\s?\(\s?(\d+?)\s?\))?")
    re_text = re.compile("(?i)(?:tiny|medium|long)?text")
    re_bytea = re.compile("(?i)bytea")
    re_timestamp = re.compile("(?i)timestamp(?:\s?\((?:(\d+?))?\))?(\swith(?:out)?\stime\szone)?")
    re_date = re.compile("(?i)date")
    re_time = re.compile("(?i)time(?:\s\((\d+?)\))?(\swith(?:out)?\stime\szone)?")
    re_interval = re.compile("(?i)interval(?:\s(year|month|day|hour|minute|second|year\sto\smonth|day\sto\shour|"
                             "day\sto\sminute|day\sto\ssecond|hour\sto\sminute|hour\sto\ssecond|minute\sto\ssecond))?"
                             "(?:\s\((\d+?)\))?")
    re_boolean = re.compile("(?i)boolean")
    re_point = re.compile("(?i)point")
    re_line = re.compile("(?i)line")
    re_lseg = re.compile("(?i)lseg")
    re_box = re.compile("(?i)box")
    re_path = re.compile("(?i)path")
    re_polygon = re.compile("(?i)polygon")
    re_circle = re.compile("(?i)circle")
    re_cidr = re.compile("(?i)cidr")
    re_inet = re.compile("(?i)inet")
    re_macaddr = re.compile("(?i)macaddr")
    # Some missing here because time
    re_uuid = re.compile("(?i)uuid")

    if re_smallint.fullmatch(t):
        return "smallint"
    elif re_integer.fullmatch(t):
        return "int"
    elif re_bigint.fullmatch(t):
        return "bigint"
    elif re_decimal.fullmatch(t):
        param = re_decimal.findall(t)
        if len(param[0][1]) != 0:
            return "decimal(%s, %s)" % (param[0][0], param[0][1])
        elif len(param[0][0]) != 0:
            return "decimal(%s)" % param[0][0]
        else:
            return "decimal"
    elif re_real.fullmatch(t):
        return "real"
    elif re_double.fullmatch(t):
        return "double precision"
    elif re_smallserial.fullmatch(t):
        return "smallserial"
    elif re_serial.fullmatch(t):
        return "serial"
    elif re_bigserial.fullmatch(t):
        return "bigserial"
    elif re_money.fullmatch(t):
        return "money"
    elif re_varchar.fullmatch(t):
        param = re_varchar.findall(t)
        r = "varchar"
        if len(param[0]) != 0:
            r += "(%s)" % param[0]
        return r
    elif re_char.fullmatch(t):
        param = re_char.findall(t)
        r = "char"
        if len(param[0][0]) != 0:
            r += "(%s)" % param[0][0]
        return r
    elif re_text.fullmatch(t):
        return "text"
    elif re_bytea.fullmatch(t):
        return "bytea"
    elif re_timestamp.fullmatch(t):
        param = re_timestamp.findall(t)
        if len(param[0][0]) != 0:
            return "timestamp (%s) %s" % (param[0][0], param[0][1])
        return "timestamp %s" % param[0][1]
    elif re_date.fullmatch(t):
        return "date"
    elif re_time.fullmatch(t):
        param = re_time.findall(t)
        if len(param[0][0]) != 0:
            return "time (%s) %s" % (param[0][0], param[0][1])
        return "time %s" % param[0][1]
    elif re_interval.fullmatch(t):
        param = re_interval.findall(t)
        if len(param[0][1]) != 0:
            return "interval %s (%s)" % (param[0][0], param[0][1])
        return "interval %s" % param[0][0]
    elif re_boolean.fullmatch(t):
        return "boolean"
    elif re_point.fullmatch(t):
        return "point"
    elif re_line.fullmatch(t):
        return "line"
    elif re_lseg.fullmatch(t):
        return "lseg"
    elif re_box.fullmatch(t):
        return "box"
    elif re_path.fullmatch(t):
        return "path"
    elif re_polygon.fullmatch(t):
        return "polygon"
    elif re_circle.fullmatch(t):
        return "circle"
    elif re_cidr.fullmatch(t):
        return "cidr"
    elif re_inet.fullmatch(t):
        return "inet"
    elif re_macaddr.fullmatch(t):
        return "macaddr"

    elif re_uuid.fullmatch(t):
        return "uuid"
    else:
        raise ValueError("Unknown SQL data type %s" % t)


class DataTable(db.Model):
    """Table in a user database"""
    # Save everything in the data_tables table
    __tablename__ = table_names["DataTable"]

    # Needed for normal operation
    # ----------------------------------------------------------------
    # Give this a unique id
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # What versions are we in?
    version_id = db.Column(db.Integer, db.ForeignKey(table_names["DataVersion"] + ".id"), nullable=False)

    # Info about children
    # --------------------------------------------
    # Columns of this table
    columns = db.relationship("DataColumn", backref="table", lazy="dynamic",
                              cascade="save-update,delete,delete-orphan,merge,expunge")

    # Metadata
    # --------------------------------------------
    # What table?
    name = db.Column(db.String, nullable=False)

    loaded = db.Column(db.Boolean, nullable=False, default=False)

    # Settings
    # ----------------------------------------------------------------
    # NOTE: these are settings like sorting order, graph type, data type, etc...

    def __init__(self, version: "DataVersion", name: str, *args, **kwargs):
        """"""
        super(DataTable, self).__init__(*args, **kwargs)

        self.name = name
        self.version_id = version.id
        self._update_db()

    def _update_db(self) -> None:
        db.session.add(self)
        db.session.commit()

    def _has_data(self) -> bool:
        return len(self.columns.all()) > 0

    def _import_error(self):
        raise TableError("Cannot import when there is already data present")

    def init_csv(self, file, **kwargs) -> None:
        """

        :param file: string with the file path or a file object
        :param kwargs: arguments are passed straight to pandas.read_csv
        :return:
        """
        if self.loaded or self._has_data():
            self._import_error()

        if type(file) is str:
            csv_file = open(file, "r")
        else:
            csv_file = file

        # Set the name
        self.name = os.path.basename(csv_file.name)

        try:
            csv_pandas = pandas.read_csv(file, **kwargs)
        except:
            # Roll back the session and re-raise the exception, one case of bare except allowed
            db.session.rollback()
            raise
        finally:
            # No matter what, the file needs to be closed
            csv_file.close()

        # Do not commit or update db here, since the load_data will already do that
        self.load_data(csv_pandas)

    def init_dump(self, columns: str, inserts: list) -> bool:
        """"""
        from .data_column import DataColumn

        re_name_type = re.compile(
            r"(?i)[\'\"`]?((?:\w|_)(?:\w|$|_|[0-9])*)[\'\"`]?\s+?("  # Start type matching
            "(?:tiny|small|big)?int(?:\(\d+?\))?|integer(?:\(\d+?\))?|decimal(?:\(\d+?\))?|"
            "numeric(?:\(\d+?\))?|real(?:\(\d+?\))?|double precision(?:\(\d+?\))?|(?:small|big|)serial|"
            "money|"
            "character varying(?:\(\d+?\))?|varchar(?:\(\d+?\))?|character(?:\(\d+?\))?|char(?:\(\d+?\))?|"
            "text|"
            "bytea|"
            "timestamp(?:\s\(\d+?\))?\swith(?:out)?\stime\szone|date|"
            "time(?:\s\(\d+?\))?(?:\swithout\stime\szone|\swith\stime\szone)?|"
            "interval(?:\s(?:year|month|day|hour|minute|second|"
            "year\sto\smonth|day\sto\shour|day\sto\sminute|day\sto\ssecond|"
            "hour\sto\sminute|hour\sto\ssecond|minute\sto\ssecond))?"
            "(?:\s\(\d+?\))?|"
            "boolean|"
            "point|line|lseg|box|path|polygon|circle|"
            "cidr|inet|macaddr|"
            "bit(?:\(\d+?\))?|bit varying(?:\(\d+?\))?|"
            "uuid|"
            "xml|"
            "json|jsonb|"
            "tsquery|tsvector)(?:(?:\[])+)?.*?,?")  # End type matching
        re_insert_names = re.compile(r"((?:\w|_)(?:\w|$|_|[0-9])*)\s*?,?\s*?")
        re_insert_tuple = re.compile(r"(\((?:(?:([\'\"`])(?:\\.|[^\\])*?\2)|true|false|NULL|[0-9.]|,*?)+?\)(?:\s?,)?)")
        re_insert_data = re.compile(r"(([\"\'`])(?:\\.|[^\\])*?\2|true|false|NULL|(?:\d+)(?:\.\d+)?)")

        q_create = db.text("CREATE TABLE tables.\"%s\" ();" % self.sql_table_name())
        q_drop = db.text("DROP TABLE IF EXISTS tables.\"%s\" CASCADE;" % self.sql_table_name())

        # Create the table
        db.session.connection().execute(q_create)

        column_types = {}

        for name, t in re_name_type.findall(columns):
            t = _match_sql_types(t)
            column_types[name] = t
            new_column: DataColumn = DataColumn(self, name)

            q_column = db.text("ALTER TABLE tables.\"%s\" ADD COLUMN \"%s\" %s DEFAULT NULL ;" %
                               (self.sql_table_name(), new_column.id, t))
            # print(q_column)
            try:
                db.session.connection().execute(q_column)
                db.session.commit()
            except (psycopg2.Warning, psycopg2.Error):
                db.session.rollback()
                db.session.execute(q_drop)
                # print("alter")
                return False

        # print(inserts)
        for columns, data in inserts:
            # print(re_insert_names.findall(columns))
            # print(columns, "\n", data)
            # print(re_insert_tuple.findall(data), sep="\n") if self.name == "organization" else None
            # print(data)

            if len(columns) == 0:
                q_insert_text = "INSERT INTO tables.\"%s\" VALUES" % self.sql_table_name()
            else:
                q_insert_text = "INSERT INTO tables.\"%s\" (%s) VALUES" % (self.sql_table_name(), columns)

            # print("test 1")
            result: re.Match = None
            for result in re_insert_tuple.finditer(data):
                data_tuple = re_insert_data.findall(result[0])
                data_string = ""
                for text in data_tuple[:-1]:
                    data_string += text[0] + ","
                data_string = data_string.rstrip(",")
                data_string = data_string.replace("\\'", "''").replace("\'0000-00-00 ", "\'0001-01-01 ")

                q_insert = db.text(q_insert_text + "(" + data_string + ");")
                try:
                    # print(q_insert)
                    db.session.connection().execute(q_insert)
                    pass
                except (psycopg2.Warning, psycopg2.Error, sqlalchemy.exc.DataError, sqlalchemy.exc.ProgrammingError):
                    db.session.rollback()
                    db.session.connection().execute(q_drop)
                    # print(result.groups())
                    # with open("temp.txt", "w") as file: file.write(str(result.groups()))
                    return False

        return True

        self._update_db()

    def init_old(self, old: "DataTable") -> dict:
        """"""
        from .data_column import DataColumn

        # Do this in SQL, since it will be faster in execution

        # Copy the table to a new table
        q = db.text(
            "CREATE TABLE tables.\"%s\" AS TABLE tables.\"%s\";" % (self.sql_table_name(), old.sql_table_name()))
        db.session.connection().execute(q)

        translate = dict()

        for column in old.columns:
            new_column = DataColumn(self, column.name)
            q = db.text(
                "ALTER TABLE tables.\"%s\" RENAME COLUMN \"%s\" TO \"%s\";" % (self.sql_table_name(), column.id,
                                                                               new_column.id))
            db.session.connection().execute(q)
            translate[column.id] = new_column.id

        return translate

    def init_pandas(self, dataframe: pandas.DataFrame) -> None:
        """"""
        if self._has_data():
            self._import_error()

        self.load_data(dataframe)

    def init_selectable(self, select: sqlalchemy.sql.expression.Select):
        from .data_column import DataColumn

        q = db.text("CREATE TABLE tables.\"%s\" AS (%s) ;" % (self.sql_table_name(), str(select)))
        db.session.connection().execute(q)

        column_q = db.text(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_schema = '%s' AND table_name = '%s' ;" % ("tables", self.sql_table_name())
        )
        result: sqlalchemy.engine.ResultProxy = db.session.connection().execute(column_q)

        if result.returns_rows and result.rowcount > 0:
            for row in result:
                new_column = DataColumn(self, row[0])
                alter_q = db.text(
                    "ALTER TABLE tables.\"%s\" "
                    "RENAME COLUMN \"%s\" TO \"%s\" ;" % (self.sql_table_name(), new_column.name, new_column.id)
                )
                db.session.connection().execute(alter_q)

        self.loaded = True
        self._update_db()

    def clear(self) -> None:
        """Clear the table of all data so we can init again"""

        db.session.connection().execute(
            "DROP TABLE IF EXISTS tables.\"%s\";" % self.sql_table_name()
        )
        for column in self.columns.all():
            db.session.delete(column)

        self.loaded = False
        self._update_db()

    def delete_column(self, column_id: int) -> "DataTable":
        """
        Delete a column from this table
        :param column_id: ID of the column to delete
        :return: self
        """
        from .data_column import DataColumn

        column: DataColumn = DataColumn.query.get(column_id)

        if column and column.table_id == self.id:
            q = db.text("ALTER TABLE tables.\"%s\" DROP COLUMN \"%s\";" % (self.sql_table_name(), column.id))
            db.session.connection().execute(q)
        else:
            raise TableError("Invalid column id")

        # Delete the column from the DB
        db.session.delete(column)

        return self

    def delete_values(self, predicate: str) -> "DataTable":
        """
        Delete values from this table based on a simple predicate and value
        :param column_id: Column to apply predicate to
        :param predicate: Predicate to apply
        :param value: Value to use for predicate
        :return: self
        """
        translate = {}
        re_columns = "(?:"
        for column in self.columns.all():
            re_columns += column.name + "|"
            translate[column.name] = column.id
        re_columns = re_columns.rstrip("|") + ")"

        regex = re.compile(r"((?:(?:(?#columns)%s|(?#numbers)(?:\d+?)(?:\.\d+?)?|(?#strings)([\"\'`])(?:\\.|[^\\])*?\2)"
                           r"(?#comperator)\s?(?:=|<|>|CONTAINS)\s?"
                           r"(?:(?#columns)%s|(?#numbers)(?:\d+?)(?:\.\d+?)?|(?#strings)([\"\'`])(?:\\.|[^\\])*?\2))"
                           r"(?:\s?(?:AND|OR)\s?(?:NOT)?\s?"
                           r"(?:(?:(?#columns)%s|(?#numbers)(?:\d+?)(?:\.\d+?)?|(?#strings)([\"\'`])(?:\\.|[^\\])*?\2)"
                           r"(?#comperator)\s?(?:=|<|>|CONTAINS)\s?"
                           r"(?:(?#columns)%s|(?#numbers)(?:\d+?)(?:\.\d+?)?|(?#strings)([\"\'`])(?:\\.|[^\\])*?\2)))?)"
                           % (re_columns, re_columns, re_columns, re_columns))

        re_banned = re.compile(r"--")

        if len(re_banned.findall()) > 0:
            raise TableError("Banned string found in predicate")

        if regex.fullmatch(predicate):
            for key in translate:
                predicate = predicate.replace(key, translate[key])
            whereclause = db.text(predicate)
            q = db.delete(self.sql_table_clause(), whereclause)
            db.session.connection().execute(q)


    def dir_name(self) -> str:
        """"""
        return self.version.dir_name() + "%s/" % self.id

    def file_name(self) -> str:
        """"""
        return self.dir_name() + "table.csv"

    def create_dir(self) -> None:
        """"""
        self.version.create_dir()
        if not os.path.exists(self.dir_name()):
            os.mkdir(self.dir_name(), 0o755)

    def load(self) -> None:
        """
        Load this version from disk
        """
        if self.loaded:
            return

        # TODO: load from disk
        raise NotImplementedError()

        # Add this to the session and commit when asked
        self.loaded = True
        self._update_db()

    def save(self, filename: str = None, **kwargs) -> None:
        """
        Save this version to a file on disk
        :param filename: Absolute filename of the file to save to
        """
        if not self.loaded:
            raise RuntimeError("Cannot save table %s to disk when it is not loaded" % self.id)

        if filename and isinstance(filename, str):
            self.get_data().to_csv(filename, index=False)
        else:
            self.create_dir()
            self.get_data().to_csv(self.file_name(), index=False, **kwargs)

        return

    def sql_table_name(self) -> str:
        """Get the internal table name"""
        return "table_%s" % self.id

    def sql_table_clause(self) -> sqlalchemy.sql.expression.TableClause:
        columns = [c.sql_column_clause() for c in self.columns.all()]
        table = sqlalchemy.sql.expression.table(self.sql_table_name(), *columns)
        table.schema = "tables"
        return table

    def sql_table(self) -> sqlalchemy.sql.expression.TableClause:
        columns = [c.sql_column() for c in self.columns.all()]
        table = sqlalchemy.sql.expression.table(self.sql_table_name(), *columns)
        table.schema = "tables"
        return table

    def select_clause(self) -> sqlalchemy.sql.expression.Select:
        """
        Get the select statement for the raw table
        :return: Select statement for the table as saved in database
        """
        return self.sql_table_clause().select()

    def select(self) -> sqlalchemy.sql.expression.Select:
        """
        Get the select statement for the table with renamed columns
        :return: Select statement for the table as seen by the user
        """
        return self.sql_table().select()

    def get_data(self) -> pandas.DataFrame:
        """
        Get the data as seen by the user
        :return: User representation of data
        """
        if not self._has_data():
            TableError("No data to get")

        return pandas.read_sql_query(self.select(), db.session.connection())

    def get_data_raw(self) -> pandas.DataFrame:
        """
        Get the raw data as stored in the db
        :return: Data as stored in DB
        """
        if not self._has_data():
            TableError("No data present")

        # Read it into a pandas thing
        return pandas.read_sql_query(self.select_clause(), db.session.connection())

    def load_data(self, dataframe: pandas.DataFrame) -> None:
        from .data_column import DataColumn

        for col in self.columns.all():
            self.columns.remove(col)
        self._update_db()

        # Rename columns and create them
        for col in dataframe:
            new_column = DataColumn(self, col)
            dataframe.rename(columns={col: str(new_column.id)}, inplace=True)

        dataframe.to_sql(self.sql_table_name(), db.session.connection(), schema="tables", if_exists="replace",
                         index=False)

        self.loaded = True
        self._update_db()
