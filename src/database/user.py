import flask_security
import typing

from .db_object import db, table_names
from .raw_tables import user_roles

if typing.TYPE_CHECKING:
    from .role import Role


def delete_user(user_id: int) -> bool:
    """
    Delete the user with the given id
    :param user_id: ID of user to delete
    :return: Success status
    """
    user: User = User.query.get(user_id)

    if user:
        db.session.delete(user)
        db.session.commit()
        return True
    else:
        return False


# Inherit user model from database model & UserMixin which provides easy-to-use foundation.
class User(db.Model, flask_security.UserMixin):
    """User class for Flask-Login"""
    # Load from the table "user"
    __tablename__ = table_names["User"]

    # Needed for normal operation
    # --------------------------------------------------------------------------
    # Each user has an ID.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    # User has a username. Unicode for obvious reasons.
    username = db.Column(db.Unicode, nullable=False, unique=True)
    # User can set a password, this is hashed
    password = db.Column(db.Unicode, nullable=False)
    # Boolean indicating whether it is an active account.
    active = db.Column(db.Boolean, nullable=False, default=True)

    # What roles does the user have? Link to role table, use user_roles for storing and
    # provide a backwards reference via users attribute
    roles = db.relationship("Role", secondary=user_roles, backref=db.backref("users", lazy="dynamic"),
                            lazy="dynamic")

    # Extra information
    # --------------------------------------------------------------------------
    # First name
    first_name = db.Column(db.Unicode, nullable=True)
    # Last name
    last_name = db.Column(db.Unicode, nullable=True)
    # E-mail address
    email = db.Column(db.Unicode, nullable=True)

    # Last login date
    last_login_at = db.Column(db.DateTime, nullable=True, default=None)
    # Current login date
    current_login_at = db.Column(db.DateTime, nullable=True, default=None)
    # Last logged in IP
    last_login_ip = db.Column(db.Unicode, nullable=True, default=None)
    # Current login IP
    current_login_ip = db.Column(db.Unicode, nullable=True, default=None)
    # Amount of times logged in
    login_count = db.Column(db.Integer, nullable=True, default=0)

    # Date account was confirmed
    confirmed_at = db.Column(db.DateTime, nullable=True, default=None)

    @property
    def databases(self):
        """
        What databases does the user have access to? Use user.databases.all() for a list
        :return: Query for databases user has access to
        """
        return self.database_access_query()

    def __init__(self, *args, **kwargs):
        super(User, self).__init__(*args, **kwargs)

        # Commit this to the database
        self._update_db()

    def _update_db(self) -> None:
        db.session.add(self)
        db.session.commit()

    def has_role(self, role: "Role") -> bool:
        """
        return 'True' if the user has the
        :param role: Role object, id or name to check
        :return: If the user has the role
        """
        from .role import Role

        if isinstance(role, Role):
            return self.roles.filter(Role.id == role.id).count() > 0
        elif isinstance(role, int):
            return self.roles.filter(Role.id == role).count() > 0
        elif isinstance(role, str):
            return self.roles.filter(Role.name == role).count() > 0
        else:
            return False

    def add_role(self, role: "Role") -> None:
        """
        Add a role to the user
        """
        # If we do not have matching rows with that role id, add it
        if not self.has_role(role):
            self.roles.append(role)
            self._update_db()

    def activate(self) -> None:
        self.active = True
        self._update_db()

    def deactivate(self) -> None:
        self.active = False
        self._update_db()

    def database_access_query(self):
        """
        Get the query that returns all databases this user has access to
        :return: query for databases with access rights
        """
        from .data import Data
        from .role import Role

        return Data.query.join(Data.access_role).filter(Role.id.in_(r.id for r in self.roles.all()))

    def database_admin_query(self):
        """
        Get the query that returns all databases this user has access to
        :return: query for databases with access rights
        """
        from .data import Data
        from .role import Role

        return Data.query.join(Data.admin_role).filter(Role.id.in_(r.id for r in self.roles.all()))
