import flask_security
import sqlalchemy

from .db_object import db, table_names


def delete_role(role_id: int) -> bool:
    """
    Delete the role with the given id
    :param role_id: ID of role to delete
    :return: Success status
    """
    role: Role = Role.query.get(role_id)

    if role:
        db.session.delete(role)
        db.session.commit()
        return True
    else:
        return False


class Role(db.Model, flask_security.RoleMixin):
    """User roles"""
    # Save everything in the roles table
    __tablename__ = table_names["Role"]

    # Make sure that a single (id, name) pair only appears once,
    # since we use 'id' to identify, but flask-security uses 'name' to do so
    __table_args__ = tuple(sqlalchemy.schema.UniqueConstraint("id", "name"))

    # Unique id for the role
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # Name of the role, cannot be NULL
    name = db.Column(db.String, nullable=False, unique=True)
    # An optional description of the role
    description = db.Column(db.String)

    def __init__(self, name: str, description: str, *args, **kwargs) -> None:
        """"""
        super(Role, self).__init__(*args, **kwargs)

        self.name = name
        self.description = description

        self._update_db()

    def _update_db(self) -> None:
        db.session.add(self)
        db.session.commit()

    def change_name(self, name: str=None) -> str:
        if name:
            self.name = name
            self._update_db()

        return self.name

    def change_description(self, description: str=None) -> str:
        if description:
            self.description = description
            self._update_db()

        return self.description
