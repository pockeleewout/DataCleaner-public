import sqlalchemy.schema
import sqlalchemy.ext.compiler
import flask_security.utils

from .db_object import db
from .user import User
from .role import Role

from .data import Data
from .data_version import DataVersion
from .data_table import DataTable
from .data_column import DataColumn

from .exceptions import *


# This code snippet fixes drop_all() on postgresql
# Credit: https://stackoverflow.com/questions/38678336/sqlalchemy-how-to-implement-drop-table-cascade#38679457
@sqlalchemy.ext.compiler.compiles(sqlalchemy.schema.DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"


def init_app(app):
    db.init_app(app)


def create_all(*args, **kwargs):
    db.create_all(*args, **kwargs)
    setup_schemas()
    create_admin_user()


def setup_schemas():
    db.session.connection().execute(sqlalchemy.schema.CreateSchema("tables"))
    db.session.commit()


def create_admin_user() -> User:
    admin = User(id=0, username="admin", password=flask_security.utils.hash_password("admin"))
    admin_role = Role(id=0, name="ADMIN", description="The one and only admin role")
    admin.add_role(admin_role)
    return admin


def drop_all():
    db.drop_all()
    drop_schemas()


def drop_schemas():
    db.session.connection().execute(sqlalchemy.schema.DropSchema("tables", cascade=True))
    db.session.commit()


if __name__ == "__main__":
    pass
