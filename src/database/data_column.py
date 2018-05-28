import sqlalchemy
import typing
import pandas

from .db_object import db, table_names

if typing.TYPE_CHECKING:
    from .data_table import DataTable


class DataColumn(db.Model):
    """Column in a Table in a user database"""
    # Save everything in the data_table_columns table
    __tablename__ = table_names["DataColumn"]
    __table_args__ = tuple(db.UniqueConstraint("table_id", "name"))

    # Needed for normal operation
    # ----------------------------------------------------------------
    # Give it a unique id
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # What table?
    table_id = db.Column(db.Integer, db.ForeignKey(table_names["DataTable"] + ".id"), nullable=False)

    # Metadata
    # --------------------------------------------
    # What column?
    name = db.Column(db.String, nullable=False)

    # Settings
    # ----------------------------------------------------------------
    # NOTE: these are settings like sorting order, graph type, data type, etc...

    def __init__(self, table: "DataTable", name: str, *args, **kwargs):
        """"""
        super(DataColumn, self).__init__(*args, **kwargs)

        # This will most likely only be called by DataTable
        self.name = name
        self.table_id = table.id
        self._update_db()

    def __str__(self):
        return self.name

    def _update_db(self) -> None:
        db.session.add(self)
        db.session.commit()

    def sql_column_clause(self) -> sqlalchemy.sql.expression.ColumnClause:
        """
        Get the raw ColumnClause of this column
        :return: raw ColumnClause
        """
        return db.column(str(self.id))

    def sql_column(self) -> sqlalchemy.sql.expression.ColumnClause:
        """
        Get the named ColumnClause of this column
        :return: labeled ColumnClause
        """
        return self.sql_column_clause().label(self.name)

    def select(self):
        """
        Get the select query for only this column, named
        :return: Select expression with named column
        """
        return db.select([self.sql_column()]).select_from(self.table.sql_table_clause())

    def select_raw(self):
        """
        Get the select query for the raw column
        :return: Select expression with original column
        """
        return db.select([self.sql_column_clause()]).select_from(self.table.sql_table_clause())

    def get_data(self) -> pandas.DataFrame:
        """
        Get the dataframe with only this columns data
        :return: The column
        """
        return pandas.read_sql_query(self.select(), db.session.connection())
