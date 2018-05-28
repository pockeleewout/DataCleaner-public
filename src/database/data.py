import typing
import uuid
import threading
import os

from .db_object import db, table_names
from .exceptions import DataError

if typing.TYPE_CHECKING:
    from .user import User
    from .data_version import DataVersion


def delete_data(data_id: int) -> None:
    """"""
    data: Data = Data.query.get(data_id)

    if data is None:
        raise RuntimeError("Cannot find data")

    # Clear all versions of the data
    data.clear()
    # Delete the data
    db.session.delete(data)
    db.session.commit()


class Data(db.Model):
    """Data class for tracking user loaded databases"""
    # Save everything in table data
    __tablename__ = table_names["Data"]

    # Needed for normal operation
    # ----------------------------------------------------------------
    # Each database has an ID.
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)

    # Keep track of versions
    versions = db.relationship("DataVersion", backref="data", order_by="DataVersion.version",
                               cascade="save-update,delete,delete-orphan,merge,expunge", lazy="dynamic")

    # What is the role that gives access?
    access_role_id = db.Column(db.Integer, db.ForeignKey(table_names["Role"] + ".id"), nullable=False)
    access_role = \
        db.relationship("Role", backref=db.backref("database_access", lazy="dynamic"),
                        foreign_keys=[access_role_id])

    # What is the role that admins this?
    admin_role_id = db.Column(db.Integer, db.ForeignKey(table_names["Role"] + ".id"), nullable=False)
    admin_role = \
        db.relationship("Role", backref=db.backref("database_admin", lazy="dynamic"),
                        foreign_keys=[admin_role_id])

    # Extra information
    # ----------------------------------------------------------------
    # Each database has a name, that isn't necessarily unique
    name = db.Column(db.Unicode, nullable=False)
    # Each database has a description
    description = db.Column(db.Text, nullable=True, default="")

    # Functions
    # ----------------------------------------------------------------

    @property
    def users(self):
        return self.sql_user_query()

    @property
    def admins(self):
        return self.sql_admin_query()

    def __init__(self, user: "User", name: str, description: str = "", in_file: str=None, *args, **kwargs):
        """
        Create user content
        :param user: Creator of the Data instance, initial owner
        :param name: Name of the Data, cannot be empty
        :param description: Optional description of the Data
        :param in_file: Filename to import from
        """
        from .role import Role

        super(Data, self).__init__(*args, **kwargs)

        # Check if arguments are good
        if (type(name) is not str) or (len(name) == 0):
            raise ValueError("name argument cannot be None or empty")

        self.name = name
        self.description = description

        # Create the new roles for this data object
        new_access_role: Role = Role(str(uuid.uuid4()), "")
        self.access_role_id = new_access_role.id
        new_admin_role: Role = Role(str(uuid.uuid4()), "")
        self.admin_role_id = new_admin_role.id

        self._update_db()

        # Add the owner as the first user with all rights
        self.add_admin(user)

        # Change the descriptions of the roles
        self.access_role.description = "[ACCESS] Role for accessing data with id %s" % self.id
        self.admin_role.description = "[ADMIN] Role for accessing data with id %s" % self.id
        db.session.add_all([self.access_role, self.admin_role])
        db.session.commit()

        # -------------------
        # Continue with everything not management related

        # If input is a valid string,
        if type(in_file) is str and len(in_file) > 4:
            # Check what file extension the filename has
            if in_file.endswith(".csv"):
                self.init_csv(in_file)
            elif in_file.endswith(".zip"):
                self.init_zip(in_file)
            elif in_file.endswith(".sql"):
                self.init_dump(in_file)

        self.busy: threading.Lock = threading.Lock()

        return

    def init_csv(self, file: str, **kwargs) -> None:
        """
        Initialize this data object with the given CSV file
        :param file: CSV file to initialize with
        """
        if self._has_versions():
            self._version_exists_error()

        # Read from a CSV file
        new_version = self.get_next_version()
        new_version.init_csv(file, **kwargs)
        # Update database
        self._update_db()

    def init_zip(self, file: str, **kwargs) -> None:
        """
        Initialize this Data object with the given zip file
        :param file: ZIP file to initialize with
        :param kwargs: Arguments to pass to pandas.read_csv
        """
        if self._has_versions():
            self._version_exists_error()

        # Read from a zip file
        new_version = self.get_next_version()
        new_version.init_zip(file, **kwargs)

        self._update_db()

    def init_dump(self, file: str) -> None:
        """
        Initialize this Data from an SQL data dump
        :param file: SQL dump file to use
        """
        if self._has_versions():
            self._version_exists_error()

        # Read from a dump
        new_version = self.get_next_version()
        new_version.init_dump(file)

        self._update_db()

    def _version_exists_error(self):
        raise DataError("There is/are already version(s) present")

    def _update_db(self) -> None:
        db.session.add(self)
        db.session.commit()

    def _has_versions(self) -> bool:
        """
        Check if this object already has versions
        :return: If it has versions
        """
        return self.versions.count() > 0

    def add_user(self, user: "User") -> None:
        """
        Add a user
        :param user: User to give access
        """
        # Add our access role to the user roles
        user.add_role(self.access_role)

    def add_admin(self, user: "User") -> None:
        """
        Add an admin to this data
        If the user is not a normal member, add that as well
        :param user: User to make admin
        """
        self.add_user(user)

        # Add the admin role to the user
        user.add_role(self.admin_role)

    def is_user_auth(self, user: "User") -> bool:
        """
        Check if this user is authorized.
        :param user: User to check.
        :return User has access to this data
        """
        return user.has_role(self.access_role)

    def is_user_owner(self, user: "User") -> bool:
        """
        Check if the user is a registered owner
        :param user: User to check
        :return User is an owner
        """
        return user.has_role(self.admin_role)

    def get_latest_version(self) -> "DataVersion":
        """
        Get the latest version of this data
        :return: DataVersion with the highest version number
        """
        from .data_version import DataVersion

        # Clear ordering, then order by version descending and get the first
        return self.versions.order_by(None).order_by(DataVersion.version.desc()).first()

    def get_next_version(self) -> "DataVersion":
        """
        Get the next version in line
        :return: New version
        """
        from .data_version import DataVersion

        # Create the new version
        version_number: int = (self.get_latest_version().version + 1) if len(self.versions.all()) > 0 else 1
        new_version = DataVersion(self, version_number)

        # Update self in the database
        self._update_db()

        return new_version

    def undo(self):
        from .data_version import DataVersion, delete_version

        if len(self.versions.all()) > 1:
            delete_version(self.get_latest_version().id)

    def clear(self) -> None:
        """
        Clear all versions of this data
        """
        for version in self.versions.all():
            version.clear()

    def dir_name(self) -> str:
        return "local/%s/" % self.id

    def create_dir(self) -> None:
        """"""
        if not os.path.exists("local/"):
            os.mkdir("local/", 0o755)
        if not os.path.exists(self.dir_name()):
            os.mkdir(self.dir_name(), 0o755)

    def sql_user_query(self) -> None:
        """
        Get the list of users with access to the data
        :return: List of users
        """
        from .user import User
        from .role import Role
        return User.query.filter(User.roles.any(Role == self.access_role))

    def sql_admin_query(self) -> None:
        """
        Get the list of users with admin level access
        :return: List of admins
        """
        from .user import User
        from .role import Role
        return User.query.filter(User.roles.any(Role == self.admin_role))
