import flask_sqlalchemy


db: flask_sqlalchemy.SQLAlchemy = flask_sqlalchemy.SQLAlchemy()

table_names = {
    "Data": "database",
    "DataVersion": "version",
    "DataTable": "table",
    "DataColumn": "column",

    "User": "user",
    "Role": "role"
}
