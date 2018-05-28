import typing
import zipfile
import os
import re
import flask
import sqlalchemy.orm.exc
import sqlalchemy.schema

from .db_object import db, table_names
from .exceptions import *

if typing.TYPE_CHECKING:
    from .data import Data


def delete_version(version_id: int) -> None:
    version: DataVersion = DataVersion.query.get(version_id)

    if version is None:
        raise RuntimeError("Cannot find version")

    version.clear()
    db.session.delete(version)
    db.session.commit()


class DataVersion(db.Model):
    """
    Keep track of a single version of a user database
    """
    __tablename__ = table_names["DataVersion"]
    __table_args__ = tuple(sqlalchemy.schema.UniqueConstraint("data_id", "version"))

    # Unique id
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # --------------------------------------------
    # What data?
    data_id = db.Column(db.Integer, db.ForeignKey(table_names["Data"] + ".id"), nullable=False)
    # What version number
    version = db.Column(db.Integer, nullable=False)

    # Is the version loaded?
    loaded = db.Column(db.Boolean, nullable=False, default=False)

    # Information about children
    # --------------------------------------------
    # Add the tables as children
    tables = db.relationship("DataTable", backref="version", lazy="dynamic",
                             cascade="save-update,delete,delete-orphan,merge,expunge")

    # Extra information
    # --------------------------------------------
    # Description of this version
    description = db.Column(db.Text, nullable=True, default="")

    def __init__(self, data: "Data", version: int=None, *args, **kwargs):
        """
        Init for DataVersion
        :param data_or_id: Data object or data id
        :param version: version to specify for this object
        """
        super(DataVersion, self).__init__(*args, **kwargs)

        # Local import because otherwise we had a circular dependency loop
        from .data import Data
        # Check if we have a valid Data object
        if type(data) is not Data:
            raise TypeError("data is not a Data object")

        self.data_id = data.id

        # Give it a new version number
        if type(version) is int:
            self.version = version
        else:
            self.version = data.get_latest_version().version + 1

        # Commit to DB
        db.session.add(self)
        db.session.commit()

    def __lt__(self, other: 'DataVersion'):
        return self.data_id <= other.data_id and self.version < other.version

    def __le__(self, other: 'DataVersion'):
        return self < other or (self.data_id <= other.data_id and self.version == other.version)

    def __gt__(self, other: 'DataVersion'):
        return self.data_id <= other.data_id and self.version > other.version

    def __ge__(self, other: 'DataVersion'):
        return self > other or (self.data_id <= other.data_id and self.version == other.version)

    def _update_db(self) -> None:
        db.session.add(self)
        db.session.commit()

    def _has_data(self) -> bool:
        """
        Check if the version has data associated with it
        :return:
        """
        return self.tables.count() > 0 or self.loaded

    def _import_error(self):
        raise VersionError("Cannot import when there is already data present")

    def init_csv(self, file: str, **kwargs):
        from .data_table import DataTable

        if self._has_data():
            self._import_error()

        new_table = DataTable(self, "table")
        self.tables.append(new_table)
        self.loaded = True
        self.description = "INIT FROM CSV %s" % os.path.basename(file)
        self._update_db()
        new_table.init_csv(file, **kwargs)

    def init_zip(self, file: str, **kwargs):
        """
        Initialize a version with a zip file
        :param file: File path to the zip file
        :param kwargs: Arguments to pass to pandas.read_csv
        :return:
        """
        from .data_table import DataTable

        if self._has_data():
            self._import_error()

        if not zipfile.is_zipfile(file):
            raise zipfile.BadZipFile("%f is not a zip file" % file)

        zip_file: zipfile.ZipFile = zipfile.ZipFile(file)

        for f in zip_file.infolist():
            path = zip_file.extract(f, flask.current_app.config["UPLOAD_FOLDER"].rstrip("/") +
                                    "/temp_%s/" % self.id)

            new_table = DataTable(self, os.path.basename(path))
            self.tables.append(new_table)
            new_table.init_csv(path, **kwargs)

        self.loaded = True
        self.description = "INIT FROM ZIP %s" % os.path.basename(file)
        self._update_db()

    def init_dump(self, filename: str):
        # TODO
        from .data_table import DataTable

        if self._has_data():
            self._import_error()

        re_create = re.compile(
            "(CREATE\sTABLE\s(?:(?:\w|_)(?:\w|$|_|[0-9])*?\.)?[\"\'`]?((?:\w|_)(?:\w|$|_|[0-9])*?)[\"\'`]?\s"
            "?\(((?:.|\s)*?)\)(?:.*?)?;)")
        re_insert = re.compile(
            "(INSERT\sINTO\s(?:(?:\w|_)(?:\w|$|_|[0-9])*?\.)?[\"\'`]?((?:\w|_)(?:\w|$|_|[0-9])*?)[\"\'`]?\s?"
            "(?:\(((?:\"?(?:(?:\w|_)(?:\w|$|_|[0-9])*?)\"?|,| )*?)\))?\sVALUES\s?((?:\(.*?\))(?:\s?,\S?\(.*?\))*?))\s?;")

        columns = dict()
        tables = dict()
        with open(filename) as file:
            for result in re_create.findall(file.read()):
                columns[result[1]] = result[2]
                tables[result[1]] = []

        with open(filename) as file:
            for result in re_insert.findall(file.read()):
                if result[1] in tables:
                    # print(result)
                    tables[result[1]].append((result[2], result[3]))

        for table in tables:
            new_table = DataTable(self, table)
            if not new_table.init_dump(columns[table], tables[table]):
                raise VersionError("Cannot import SQL dump file \"%s\"" % filename)

    def init_old(self, old: "DataVersion") -> dict:
        # TODO
        from .data_table import DataTable

        translate = {}

        for table in old.tables:
            new_table = DataTable(self, table.name)
            translate[table.id] = (new_table.id, new_table.init_old(table))
        self.description = "INIT FROM OLD VERSION %s(id=%s)" % (old.version, old.id)
        self._update_db()
        return translate

    def clear(self) -> None:
        """
        Clear all the tables in this version
        """
        for table in self.tables.all():
            table.clear()

    def dir_name(self) -> str:
        return self.data.dir_name() + ("%s/" % self.id)

    def create_dir(self) -> None:
        """"""
        self.data.create_dir()
        if not os.path.exists(self.dir_name()):
            os.mkdir(self.dir_name(), 0o755)

    def load(self) -> None:
        """
        Load this version from disk
        """
        # If we are already loaded, good!
        if self.loaded:
            return

        # TODO: implement
        # We can cache everything in files for each version
        raise NotImplementedError()

        # We have loaded everything, push this to the database
        self.loaded = True
        self._update_db()

    def unload(self) -> None:
        """
        Unload this version from the database
        """
        # Check if we aren't already unloaded
        if not self.loaded:
            return

        # TODO
        raise NotImplementedError()

        self.loaded = False
        self._update_db()

    def save(self) -> None:
        """
        Save this version to disk
        """
        if not self.loaded:
            raise RuntimeError("Cannot save version %s when it is not loaded" % self.id)

        # TODO: implement
        # We can load the cached version from disk
        raise NotImplementedError()

    def join(self, table_ids: list, *args, name: str="JOIN") -> bool:
        """
        Join 2 or more tables in this version

        table_ids is a list of table ids for the tables to join
        args are lists of columns ids

        example:
            You have 2 tables people and employees, you want to join on columns people.age and employees.age
            Take people has ID=1 and employees has ID=2, people.age has ID=4 and employees.age has ID=18

            You call the function as follows:
                version.join([1, 2], [4, 18])
            Where [1, 2] is the tables and [4, 18] are the columns

        This will also work for multi-table joins,
        example:
            version.join([2, 6, 17], [3, 54, 7], [12, 5, 9])

            This will join tables with ids 2, 6, 17 where
            columns 3, 54, 7 match while columns 12, 5, 9 match

        If you want to join multiple tables but don't want to match a column in all,
        place a None in the position of the table excluded from that match
        example:
            version.join([1, 2, 3], [1, 2, None], [None, 3, 4])

            This will match columns 1 and 2 from tables 1 and 2 and
            match columns 3 and 4 from tables 2 and 3 respectively

        :param table_ids: List of table ids in this version
        :param args: lists with all column ids that need to match in the tables
        :return: Success status
        """
        from .data_table import delete_table, join_tables, DataTable
        # print(table_ids)

        # Check if the tables belongs in this version
        for table_id in table_ids:
            # print(self.tables.all())
            if self.tables.filter(DataTable.id == table_id).count() <= 0:
                # print(table_id)
                return False

        join_clause = join_tables(table_ids, *args)

        tables = [DataTable.query.get(x) for x in table_ids]
        columns = []
        for table in tables:
            columns.extend([column.sql_column_clause().label(table.name+"."+column.name) for column in table.columns.all()])

        select: sqlalchemy.sql.expression.Select = \
            sqlalchemy.sql.expression.select(columns).select_from(join_clause)
        print(select)

        new_table: DataTable = DataTable(self, name)
        new_table.init_selectable(select)

        # Delete the old tables used for the join
        for table_id in table_ids:
            delete_table(table_id)
