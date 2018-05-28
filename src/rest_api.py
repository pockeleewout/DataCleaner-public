import flask
import flask_restful
import flask_security
import sqlalchemy.orm.query
import pandas
import werkzeug.datastructures

from database import db, Data, DataVersion, DataTable, DataColumn, Role, User, TableError
from database.data import delete_data
import transform


def _get_from_request(prop: str, request: flask.Request = flask.request):
    """
    Try getting some argument from a request, either query strings or json
    :param prop: Property to get
    :param request: Request to get from
    :return: Requested value or None
    """
    req_json = request.get_json()

    if prop in request.args:
        return request.args[prop]
    elif req_json and prop in req_json:
        return req_json[prop]

    return None


def _is_admin(user: User) -> bool:
    """
    Check if user has admin role
    :param user: User object
    :return: is admin?
    """
    return user.has_role(Role.query.get(0))


def _none_status(obj, user: User = flask_security.current_user, fault_status: int = 400) -> None:
    """
    Check if object is None and return the appropriate response depending if user is admin or not
    :param obj: Object to check if None
    :param user: User to check admin status
    :param fault_status: Status to return if None and user is admin
    """
    if obj is None:
        if _is_admin(user):
            flask.abort(fault_status)
        else:
            flask.abort(403)
    return


def _int(value, user: User = flask_security.current_user):
    """
    Convert to int with flask aborts
    :param value: Value to convert
    :param user: User to check admin status
    :return:
    """
    try:
        value = int(value)
    except (ValueError, TypeError):
        flask.abort(400) if _is_admin(user) else flask.abort(403)
    return value


def _float(value, user: User = flask_security.current_user):
    """
    Convert to float with flask aborts
    :param value: Value to convert
    :param user: User to check admin status
    :return:
    """
    try:
        value = float(value)
    except (ValueError, TypeError):
        flask.abort(400) if _is_admin(user) else flask.abort(403)
    return value


def _dict_query(query: sqlalchemy.orm.query.Query, depth: int = 0, extra: bool = False) -> dict:
    """
    Convert a query to a dict of objects
    :param query: Query to list
    :param depth: Depth to query objects at
    :param extra: Include extra info?
    :return: Dict with data and size
    """
    result = []

    for obj in query.all():
        if type(obj) is Data:
            result.append(_dict_data(obj, depth, extra))
        elif type(obj) is DataVersion:
            result.append(_dict_version(obj, depth, extra))
        elif type(obj) is DataTable:
            result.append(_dict_table(obj, depth, extra))
        elif type(obj) is DataColumn:
            result.append(_dict_column(obj, depth, extra))
        elif type(obj) is User:
            result.append(_dict_user(obj, depth, extra))
        elif type(obj) is Role:
            result.append(_dict_role(obj, depth, extra))
        else:
            raise TypeError("Unsupported type: %s" % str(type(obj)))

    return {"data": result, "size": len(result)}


def _dict_column(column: DataColumn, depth: int = 0, extra: bool = False) -> dict:
    """
    Convert a column to a dict
    :param column: Column object to convert
    :param depth: Depth to convert at
    :param extra: Extra info
    :return: Dict representation of DataColumn
    """
    data = {
        "id": column.id,
        "name": column.name
    }

    if depth > 0:
        pass

    if extra:
        data["table_id"] = column.table_id

    return data


def _dict_data(data: Data, depth: int = 0, extra: bool = False) -> dict:
    """
    Convert a Data object to a dict representation
    :param data: Data object
    :param depth: Depth to convert at
    :param extra: Extra information
    :return: Dict version of Data
    """
    d = {
        "id": data.id,
        "name": data.name,
        "description": data.description,
        "access_role": _dict_role(data.access_role, max(depth - 1, 0)),
        "admin_role": _dict_role(data.admin_role, max(depth - 1, 0))
    }

    if depth > 0:
        d["versions"] = _dict_query(data.versions, max(depth - 1, 0))
        d["users"] = _dict_query(data.users, max(depth - 1, 0))

    if extra:
        pass

    return d


def _dict_role(role: Role, depth: int = 0, extra: bool = False) -> dict:
    """
    Convert a Role object to a dict representation
    :param role: Role object
    :param depth: Depth to convert at
    :param extra: Extra information
    :return: Dict version of Role
    """
    data = {
        "id": role.id,
        "name": role.name,
        "description": role.description
    }

    if depth > 0:
        data["users"] = _dict_query(role.users, max(depth - 1, 0))

    if extra:
        pass

    return data


def _dict_table(table: DataTable, depth: int = 0, extra: bool = False) -> dict:
    """
    Convert a DataTable object to a dict representation
    :param table: Table object
    :param depth: Depth to convert at
    :param extra: Extra information
    :return: Dict version of DataTable
    """
    data = {
        "id": table.id,
        "name": table.name
    }

    if depth > 0:
        data["columns"] = _dict_query(table.columns, max(depth - 1, 0))

    if extra:
        data["version_id"] = table.version_id
        data["loaded"] = table.loaded

    return data


def _dict_user(user: User, depth: int = 0, extra: bool = False) -> dict:
    """
    Convert User object to a dict representation
    :param user: User object
    :param depth: Depth to convert at
    :param extra: Extra information
    :return: Dict version of User
    """
    data = {
        "id": user.id,
        "username": user.username,
        "active": user.active,
        "firstname": user.first_name,
        "lastname": user.last_name
    }

    if depth > 0:
        data["roles"] = _dict_query(user.roles, max(depth - 1, 0))
        data["databases"] = _dict_query(user.databases, max(depth - 1, 0))

    if extra:
        pass

    return data


def _dict_version(version: DataVersion, depth: int = 0, extra: bool = False) -> dict:
    """
    Convert DataVersion to a dict representation
    :param version: DataVersion object
    :param depth: Depth to convert at
    :param extra: Extra information
    :return: Dict version of DataVersion
    """
    data = {
        "id": version.id,
        "version": version.version,
        "description": version.description
    }

    if depth > 0:
        data["tables"] = _dict_query(version.tables, max(depth - 1, 0), extra)

    if extra:
        data["data_id"] = version.data_id
        data["loaded"] = version.loaded

    return data


# Resource classes
# ------------------------------------------------------------------------------

class RestColumnById(flask_restful.Resource):
    @staticmethod
    def get(column_id):
        """
        Get information of a column based on its id
        :param column_id: ID of the column to get info from
        :return: JSON object representing a DataColumn
        """
        column_id = _int(column_id, flask_security.current_user)

        column: DataColumn = DataColumn.query.get(column_id)
        _none_status(column)

        # If the user is not authorized, return 403 Forbidden
        if not column.table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_column(column, 1))


class RestDatabase(flask_restful.Resource):
    @staticmethod
    def get():
        """
        Get a list of all databases on the server
        :return: JSON list with all JSON objects for Data objects
        """
        # Check if user is admin, if not return 403 Forbidden
        if not _is_admin(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_query(Data.query))


class RestDatabaseById(flask_restful.Resource):
    @staticmethod
    def get(data_id):
        """
        Get info about a database based on its ID
        :param data_id: ID of Data object
        :return: JSON object representing a Data instance
        """
        data_id = _int(data_id, flask_security.current_user)

        data: Data = Data.query.get(data_id)
        _none_status(data)

        # If the user is not authorized, return 403 Forbidden
        if not data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_data(data, 1))

    @staticmethod
    def delete(data_id):
        """
        Delete a database based on its ID
        :param data_id:
        :return: 204 No Content | 403 Forbidden | 400 Bad Request
        """

        data_id = _int(data_id, flask_security.current_user)

        data: Data = Data.query.get(data_id)
        _none_status(data)

        # If the user is not authorized, return 403 Forbidden
        if not data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        try:
            delete_data(data_id)
            return "", 204
        except RuntimeError:
            flask.abort(404)

    @staticmethod
    def post(data_id):
        """
        Change database details with a POST request
        :param data_id: ID of the data to change
        :return: 204 No Content | 403 Forbidden | 400 Bad Request
        """
        data_id = _int(data_id, flask_security.current_user)

        data: Data = Data.query.get(data_id)
        _none_status(data)

        # If the user is not admin on this data, return 403 Forbidden
        if not data.is_user_owner(flask_security.current_user):
            flask.abort(403)

        name = _get_from_request("name")
        description = _get_from_request("description")

        change: bool = False

        if name:
            data.name = name
            change = True
        if description:
            data.description = description
            change = True

        if change:
            db.session.add(data)
            db.session.commit()

        return "", 204


class RestDatabaseUsers(flask_restful.Resource):
    @staticmethod
    def get(data_id):
        """
        Get a list of users for a Data instance
        :param data_id: ID of Data
        :return: List of users with access to Data instance
        """
        data_id = _int(data_id, flask_security.current_user)

        data = Data.query.get(data_id)
        _none_status(data)

        # If the user is not authorized, return 403 Forbidden
        if not data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_query(data.users))

    @staticmethod
    def post(data_id):
        """
        Add a user to the data object
        :param data_id: ID of data to add user to
        :return: 204 No Content | 403 Forbidden | 400 Bad Request
        """
        data_id = _int(data_id, flask_security.current_user)

        data: Data = Data.query.get(data_id)
        _none_status(data)

        if not data.is_user_owner(flask_security.current_user):
            flask.abort(403)

        user_id = _int(_get_from_request("user_id"), flask_security.current_user)
        admin = _get_from_request("admin")

        user: User = User.query.get(user_id)
        _none_status(user)

        data.add_user(user)
        if admin:
            data.add_admin(user)

        return "", 204


class RestRole(flask_restful.Resource):
    @staticmethod
    def get():
        """"""
        # Check if user is admin, if not return 403 Forbidden
        if not _is_admin(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_query(Role.query))


class RestRoleById(flask_restful.Resource):
    @staticmethod
    def get(role_id):
        """"""
        role_id = _int(role_id, flask_security.current_user)

        # Check if user is admin, if not return 403 Forbidden
        if not _is_admin(flask_security.current_user):
            flask.abort(403)

        role: Role = Role.query.get(role_id)
        _none_status(role)

        return flask.jsonify(_dict_role(role, 1))

    @staticmethod
    def post(role_id):
        """
        Change the details of the role with a POST request
        :param role_id: ID of the role to change
        :return: 204 No Content | 403 Forbidden | 400 Bad Request
        """
        role_id = _int(role_id, flask_security.current_user)

        if not _is_admin(flask_security.current_user):
            flask.abort(403)

        role: Role = Role.query.get(role_id)
        _none_status(role)

        name = _get_from_request("name")
        description = _get_from_request("description")

        change: bool = False

        if name:
            role.name = name
            change = True
        if description:
            role.description = description
            change = True

        if change:
            db.session.add(role)
            db.session.commit()

        return "", 204


class RestRoleUsers(flask_restful.Resource):
    @staticmethod
    def get(role_id):
        """"""
        role_id = _int(role_id, flask_security.current_user)

        # Check if user is admin, if not return 403 Forbidden
        if not _is_admin(flask_security.current_user):
            flask.abort(403)

        role = Role.query.get(role_id)
        _none_status(role)

        return flask.jsonify(_dict_query(role.users))

    @staticmethod
    def post(role_id):
        """"""
        role_id = _int(role_id, flask_security.current_user)

        if not _is_admin(flask_security.current_user):
            flask.abort(403)

        role: Role = Role.query.get(role_id)
        _none_status(role)

        user_id = _int(_get_from_request("user_id"), flask_security.current_user)
        user: User = User.query.get(user_id)
        _none_status(user)

        user.add_role(role)

        return "", 204


class RestTable(flask_restful.Resource):
    @staticmethod
    def get():
        """"""
        # Check if user is admin, if not return 403 Forbidden
        if not _is_admin(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_query(DataTable.query))


class RestTableById(flask_restful.Resource):
    @staticmethod
    def get(table_id):
        """"""
        table_id = _int(table_id, flask_security.current_user)

        table: DataTable = DataTable.query.get(table_id)
        _none_status(table)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_table(table, 1))

    @staticmethod
    def post(table_id):
        """"""
        table_id = _int(table_id, flask_security.current_user)

        table: DataTable = DataTable.query.get(table_id)
        _none_status(table)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_owner(flask_security.current_user):
            flask.abort(403)

        name = _get_from_request("name")

        if name:
            table.name = name
            db.session.add(table)
            db.session.commit()

        return "", 204


class RestTableColumns(flask_restful.Resource):
    @staticmethod
    def get(table_id):
        """"""
        table_id = _int(table_id, flask_security.current_user)

        table = DataTable.query.get(table_id)
        _none_status(table)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_query(table.columns))


def _verify_datatables_request(request):
    if "draw" not in request:
        return None

    if "start" not in request or type(request["start"]) is not int:
        request["start"] = 0
    if "length" not in request or type(request["length"]) is not int:
        request["length"] = 10
    if "order" not in request or type(request["order"]) is not list:
        request["order"] = []
    else:
        i = len(request["order"])
        while i > 0:
            i -= 1
            if "column" not in request["order"][i] or type(request["order"][i]["column"]) is not int:
                del request["order"][i]
            elif "dir" not in request["order"][i] or type(request["order"][i]["dir"]) is not str:
                del request["order"][i]
    if "columns" not in request or type(request["columns"]) is not dict:
        request["columns"] = {}

    return request


class RestTableContent(flask_restful.Resource):
    @staticmethod
    def post(table_id):
        table_id = _int(table_id, flask_security.current_user)

        # Try using input
        table: DataTable = DataTable.query.get(table_id)
        _none_status(table)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        json = flask.request.get_json()
        _none_status(json)
        json = _verify_datatables_request(json)

        q = table.select_clause()
        records = db.session.connection().execute(q).rowcount

        response = {"draw": int(json["draw"]), "recordsTotal": records, "recordsFiltered": records, "data": []}

        q = q.offset(json["start"]).limit(json["length"])

        # Check ordering
        for order in json["order"]:
            if order["dir"].lower() == "asc":
                q = q.order_by(table.columns.all()[order["column"]].sql_column_clause().asc())
            else:
                q = q.order_by(table.columns.all()[order["column"]].sql_column_clause().desc())

        dataframe = pandas.read_sql_query(q, db.session.connection())
        data = []
        for x in dataframe:
            data.append(dataframe[x].values.tolist())
        response["data"] = dataframe.as_matrix().tolist()

        # Return the jsonified dataframe
        return flask.jsonify(response)


class RestTransformJoin(flask_restful.Resource):
    @staticmethod
    def post():
        data_id = _int(_get_from_request("db_id"))
        table_1_id = _int(_get_from_request("table_1"))
        table_2_id = _int(_get_from_request("table_2"))
        columns_1 = _get_from_request("columns_1")
        columns_2 = _get_from_request("columns_2")
        name = _get_from_request("name")

        data: Data = Data.query.get(data_id)
        _none_status(data)
        table_1: DataTable = DataTable.query.get(table_1_id)
        _none_status(table_1)
        table_2: DataTable = DataTable.query.get(table_2_id)

        if not data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        if type(columns_1) != list:
            columns_1 = None
        if type(columns_2) != list:
            columns_2 = None
        if len(columns_1) != len(columns_2):
            columns_1 = None
            columns_2 = None
        # Use already available response handling
        _none_status(columns_1)
        _none_status(columns_2)

        column_join = []
        on_string = ""
        i = 0
        while i < len(columns_1):
            c1 = table_1.columns.filter(DataColumn.name == columns_1[i]).first()
            c2 = table_2.columns.filter(DataColumn.name == columns_2[i]).first()
            if c1 is None or c2 is None:
                flask.abort(400)

            column_join.append([c1.id, c2.id])
            on_string += str(columns_1[i]) + "=" + str(columns_2[i]) + ","
            i += 1
        on_string = on_string.rstrip(",")

        version = data.get_latest_version()
        new_version: DataVersion = data.get_next_version()
        translate = new_version.init_old(version)
        new_version.description = "JOIN TABLES %s AND %s ON %s" % (table_1.name, table_2.name, on_string)
        db.session.add(new_version)
        db.session.commit()

        # Translate column join
        new_column_join = []
        for join in column_join:
            new_column_join.append([translate[table_1_id][1][join[0]], translate[table_2_id][1][join[1]]])
        column_join = new_column_join

        result = new_version.join([translate[table_1_id][0], translate[table_2_id][0]], *column_join, name)

        if result:
            return "", 204
        else:
            return "", 400


class RestUser(flask_restful.Resource):
    @staticmethod
    def get():
        """"""
        # Check if user is admin, if not return 403 Forbidden
        if not _is_admin(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_query(User.query))


class RestUserById(flask_restful.Resource):
    @staticmethod
    def get(user_id):
        """"""
        user_id = _int(user_id, flask_security.current_user)

        # Check if user is admin or the user himself, if not return 403 Forbidden
        if not _is_admin(flask_security.current_user) and flask_security.current_user.id != user_id:
            flask.abort(403)

        user = User.query.get(user_id)
        _none_status(user)

        return flask.jsonify(_dict_user(user, 1))

    @staticmethod
    def post(user_id):
        """
        Change user details with a POST request
        :param user_id: ID of the user to change
        :return: 204 No Content | 403 Forbidden | 400 Bad Request
        """
        user_id = _int(user_id, flask_security.current_user)

        if not _is_admin(flask_security.current_user) and flask_security.current_user.id != user_id:
            flask.abort(403)

        user: User = User.query.get(user_id)
        _none_status(user)

        username = _get_from_request("username", flask.request)
        password = _get_from_request("password", flask.request)
        active = _get_from_request("active", flask.request)
        firstname = _get_from_request("fname", flask.request)
        lastname = _get_from_request("lname", flask.request)
        email = _get_from_request("email", flask.request)
        admin = _get_from_request("admin")

        change: bool = False

        if username:
            user.username = username
            change = True
        if password:
            user.password = flask_security.utils.hash_password(password)
            change = True
        if active:
            user.active = bool(active == "on")
            change = True
        if firstname:
            user.first_name = firstname
            change = True
        if lastname:
            user.last_name = lastname
            change = True
        if email:
            user.email = email
            change = True
        if admin:
            user.add_role(Role.query.get(0))

        if change:
            db.session.add(user)
            db.session.commit()

        return "", 204


class RestUserDatabase(flask_restful.Resource):
    @staticmethod
    def get(user_id):
        """"""
        user_id = _int(user_id, flask_security.current_user)

        # Check if user is admin or the user himself, if not return 403 Forbidden
        if not _is_admin(flask_security.current_user) and flask_security.current_user.id != user_id:
            flask.abort(403)

        user = User.query.get(user_id)
        _none_status(user)

        return flask.jsonify(_dict_query(user.databases))


class RestUserRole(flask_restful.Resource):
    @staticmethod
    def get(user_id):
        """"""
        user_id = _int(user_id, flask_security.current_user)

        # Check if user is admin, if not return 403 Forbidden
        if not _is_admin(flask_security.current_user) and flask_security.current_user.id != user_id:
            flask.abort(403)

        user = User.query.get(user_id)
        _none_status(user)

        return flask.jsonify(_dict_query(user.roles))


class RestVersionById(flask_restful.Resource):
    @staticmethod
    def get(version_id):
        """"""
        version_id = _int(version_id, flask_security.current_user)

        version: DataVersion = DataVersion.query.get(version_id)
        _none_status(version)

        # If the user is not authorized, return 403 Forbidden
        if not version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_version(version, 1))


class RestVersionTables(flask_restful.Resource):
    @staticmethod
    def get(version_id):
        """"""
        version_id = _int(version_id, flask_security.current_user)

        version = DataVersion.query.get(version_id)
        _none_status(version)

        # If the user is not authorized, return 403 Forbidden
        if not version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        return flask.jsonify(_dict_query(version.tables))


# Transformations
# ======================================

class RestTransformChangeType(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        new_type = _get_from_request("radio", flask.request)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if None in [column_name, new_type]:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.change_type(df, column_name, new_type)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "CHANGE TYPE OF %s TO %s" % (column_name, new_type)
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformDeduplication(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        max_distance = _int(_get_from_request("edit_distance", flask.request), flask_security.current_user)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if column_name is None:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        duplicates = transform.find_duplicates(df, column_name, max_distance)

        # Return 204 No Content
        return "", 204


class RestTransformDeduplicationResult(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        nr_strings = _int(_get_from_request("nr_strings", flask.request), flask_security.current_user)
        chain = _get_from_request("chain", flask.request) == "on"

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if column_name is None:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Process additional input
        to_replace: {str, str} = {}
        for i in range(1, nr_strings + 1):
            if _get_from_request("replace_%i" % i, flask.request):
                string = _get_from_request("string_%i" % i, flask.request)
                replacement = _get_from_request("replacement_%i" % i, flask.request)

                # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
                if None in [string, replacement]:
                    flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

                # Add replace rule to the dictionary
                to_replace[string] = replacement

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.replace_duplicates(df, column_name, to_replace, chain)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "REPLACE DUPLICATES IN %s" % column_name
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformDeleteColumn(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("column_name", flask.request)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if None in [column_name]:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the actual operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "DELETE COLUMN %s" % column_name
        new_table: DataTable = DataTable(new_version, table.name)
        translate = new_table.init_old(table)
        # new_table.delete_column(translate[column.id]) TODO

        # Return 204 No Content
        return "", 204


class RestTransformDiscretizeEquiDistance(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        nr_bins = _int(_get_from_request("nr_bins", flask.request), flask_security.current_user)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if column_name is None:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.discretize_equiwidth(df, column_name, nr_bins)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "DISCRETIZE (EQUIWIDTH) TO %i BINS IN %s" % (nr_bins, column_name)
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformDiscretizeEquiFrequency(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        nr_bins = _int(_get_from_request("nr_bins", flask.request), flask_security.current_user)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if column_name is None:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.discretize_equifreq(df, column_name, nr_bins)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "DISCRETIZE (EQUIFREQ) TO %i BINS IN %s" % (nr_bins, column_name)
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformDiscretizeRanges(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        nr_bins = _int(_get_from_request("nr_bins", flask.request), flask_security.current_user)

        boundaries = []
        for i in range(1, nr_bins):
            boundary = _float(_get_from_request("input_%i" % i, flask.request), flask_security.current_user)
            boundaries.append(boundary)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if column_name is None:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.discretize_ranges(df, column_name, boundaries)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "DISCRETIZE (MANUAL RANGES) TO %i BINS IN %s" % (nr_bins, column_name)
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformExtractDateTime(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        to_extract = _get_from_request("radio", flask.request)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if None in [column_name, to_extract]:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.extract_from_datetime(df, column_name, to_extract[5:])

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "EXTRACT %s FROM DATETIME IN %s" % (to_extract, column_name)
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformFillEmpty(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        fill_with = _get_from_request("radio", flask.request)
        value = _get_from_request("replace_with", flask.request)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if None in [column_name, fill_with, value]:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        if fill_with == "mean":
            df = transform.fill_empty_mean(df, column_name)
        elif fill_with == "median":
            df = transform.fill_empty_median(df, column_name)
        elif fill_with == "value":
            df = transform.fill_empty_value(df, column_name, value)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "FILL EMPTY WITH %s IN %s" % (fill_with, column_name)
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformFindReplace(flask_restful.Resource):
    @staticmethod
    def post():
        # Set response headers.
        resp = flask.Response()
        resp.headers['Access-Control-Allow-Origin'] = '*'

        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        from_data = _get_from_request("Find", flask.request)
        to_data = _get_from_request("Replace", flask.request)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if None in [column_name, from_data, to_data]:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.find_replace(df, column_name, from_data, to_data)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "FIND %s REPLACE %s IN %s" % (from_data, to_data, column_name)
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Respond
        return flask.Response(
            headers=werkzeug.datastructures.Headers(
                [
                    ('Access-Control-Allow-Origin', '*'),
                    ('Access-Control-Allow-Credentials', 'True')
                ]
            ),
            status=204
        )


class RestTransformFindReplaceRegex(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        from_data = _get_from_request("Find", flask.request)
        to_data = _get_from_request("Replace", flask.request)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if None in [column_name, from_data, to_data]:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.find_replace_regex(df, column_name, from_data, to_data)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "FIND %s REPLACE %s IN %s" % (from_data, to_data, column_name)
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformNormalize(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if column_name is None:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.normalize(df, column_name)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "NORMALIZE %s" % column_name
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformOneHotEncoding(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        use_old_name = _get_from_request("use_old", flask.request) == "on"

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if column_name is None:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the database
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.one_hot_encode(df, column_name, use_old_name)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "ONE-HOT ENCODE %s" % column_name
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformRemoveOutliers(flask_restful.Resource):
    @staticmethod
    def post():
        # Process input
        database_id = _int(_get_from_request("db_id", flask.request), flask_security.current_user)
        column_name = _get_from_request("colinfo", flask.request)
        outside_range = _float(_get_from_request("range", flask.request), flask_security.current_user)

        # If input invalid, return 400 Bad Request if admin, 403 Forbidden otherwise
        if column_name is None:
            flask.abort(400) if _is_admin(flask_security.current_user) else flask.abort(403)

        # Try getting the table
        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table, flask_security.current_user, 422)

        # If the user is not authorized, return 403 Forbidden
        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        # Do the operation
        df: pandas.DataFrame = table.get_data()
        df = transform.remove_outliers(df, column_name, outside_range)

        # Write the operation
        new_version: DataVersion = table.version.data.get_next_version()
        new_version.description = "REMOVE OUTLIERS WITH RANGE %f IN %s" % (outside_range, column_name)
        new_table: DataTable = DataTable(new_version, table.name)
        new_table.init_pandas(df)

        # Return 204 No Content
        return "", 204


class RestTransformDelete(flask_restful.Resource):
    @staticmethod
    def post():
        # process input
        database_id = _int(_get_from_request("db_id"))
        predicate = _get_from_request("predicate")

        version: DataVersion = Data.query.get(database_id).get_latest_version()
        table: DataTable = version.tables.filter(DataTable.loaded).first()
        _none_status(table)

        if not table.version.data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        try:
            table.delete_values(predicate)
        except TableError:
            flask.abort(400)

        return "", 204


class RestTransformUndo(flask_restful.Resource):
    @staticmethod
    def post():
        # process input
        database_id = _int(_get_from_request("db_id"))

        data: Data = Data.query.get(database_id)
        _none_status(data)

        if not data.is_user_auth(flask_security.current_user):
            flask.abort(403)

        data.undo()

        return "", 204


# Flask-RESTFul
# ==============================================================================

# create a Flask-RESTFul API in a blueprint
restful_blueprint: flask.Blueprint = flask.Blueprint("rest_api", __name__, url_prefix="/api/v1")
restful_api: flask_restful.Api = flask_restful.Api(restful_blueprint,
                                                   decorators=[flask_security.login_required])

# Add endpoints
restful_api.add_resource(RestColumnById, "/column/<column_id>/")
restful_api.add_resource(RestDatabase, "/database/")
restful_api.add_resource(RestDatabaseById, "/database/<data_id>/")
restful_api.add_resource(RestDatabaseUsers, "/database/<data_id>/user/")
restful_api.add_resource(RestRole, "/role/")
restful_api.add_resource(RestRoleById, "/role/<role_id>/")
restful_api.add_resource(RestRoleUsers, "/role/<role_id>/user/")
restful_api.add_resource(RestTableById, "/table/<table_id>/")
restful_api.add_resource(RestTableColumns, "/table/<table_id>/column/")
restful_api.add_resource(RestTableContent, "/table/<table_id>/content/")
restful_api.add_resource(RestUser, "/user/")
restful_api.add_resource(RestUserById, "/user/<user_id>/")
restful_api.add_resource(RestUserDatabase, "/user/<user_id>/database/")
restful_api.add_resource(RestUserRole, "/user/<user_id>/role/")
restful_api.add_resource(RestVersionById, "/version/<version_id>/")
restful_api.add_resource(RestVersionTables, "/version/<version_id>/table/")

restful_api.add_resource(RestTransformChangeType, "/transform/change_type/")
restful_api.add_resource(RestTransformDeduplication, "/transform/deduplication")
restful_api.add_resource(RestTransformDeduplicationResult, "/transform/deduplication_result")
restful_api.add_resource(RestTransformDeleteColumn, "/transform/delete_column/")
restful_api.add_resource(RestTransformDiscretizeEquiDistance, "/transform/discretize_distance/")
restful_api.add_resource(RestTransformDiscretizeEquiFrequency, "/transform/discretize_frequency/")
restful_api.add_resource(RestTransformDiscretizeRanges, "/transform/discretize_ranges")
restful_api.add_resource(RestTransformExtractDateTime, "/transform/extract_date_time/")
restful_api.add_resource(RestTransformFillEmpty, "/transform/fill_empty/")
restful_api.add_resource(RestTransformFindReplace, "/transform/find_replace/")
restful_api.add_resource(RestTransformFindReplaceRegex, "/transform/find_replace_regex/")
restful_api.add_resource(RestTransformNormalize, "/transform/normalize/")
restful_api.add_resource(RestTransformOneHotEncoding, "/transform/one_hot_encoding/")
restful_api.add_resource(RestTransformRemoveOutliers, "/transform/remove_outliers/")
restful_api.add_resource(RestTransformJoin, "/transform/join/")
restful_api.add_resource(RestTransformDelete, "/transform/delete/")
restful_api.add_resource(RestTransformUndo, "/transform/undo/")
