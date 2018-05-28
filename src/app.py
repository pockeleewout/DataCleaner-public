import os
import flask
import flask_security
import numpy as np
import werkzeug.utils
import pandas


from app_object import flask_app as _app
import database
import transform
import rest_api
import security


# Error handling
# ------------------------------------------------------------------------------

@_app.errorhandler(400)
def page_not_found(e):
    return flask.render_template('CustomErrors/400.html'), 400


@_app.errorhandler(401)
def page_not_found(e):
    return flask.render_template('CustomErrors/401.html'), 401


@_app.errorhandler(403)
def page_not_found(e):
    return flask.render_template('CustomErrors/403.html'), 403


@_app.errorhandler(404)
def page_not_found(e):
    return flask.render_template('CustomErrors/404.html'), 404


@_app.errorhandler(422)
def page_not_found(e):
    return flask.render_template('CustomErrors/422.html'), 422


@_app.errorhandler(500)
def page_not_found(e):
    return flask.render_template('CustomErrors/500.html'), 500


@_app.errorhandler(502)
def page_not_found(e):
    return flask.render_template('CustomErrors/502.html'), 502


@_app.errorhandler(503)
def page_not_found(e):
    return flask.render_template('CustomErrors/503.html'), 503


@_app.errorhandler(504)
def page_not_found(e):
    return flask.render_template('CustomErrors/504.html'), 504


# Routing
# ------------------------------------------------------------------------------

@_app.route('/')
def index_base():
    return flask.render_template('./Home/index.html',
                                 logged=flask_security.current_user.is_authenticated)


@_app.route('/databases')
@flask_security.login_required
def database_base():
    return flask.render_template('./Databases/index.html')


@_app.route('/databases/view/<db_id>', methods=['POST', 'GET'])
@flask_security.login_required
def view_database(db_id):
    table_data: database.Data = database.Data.query.get(db_id)

    if table_data.get_latest_version().tables.count() > 1:
        return flask.redirect(flask.url_for("join", data_id=db_id))

    datatable = table_data.get_latest_version().tables.first()
    if table_data is None:
        return flask.abort(404)

    if not table_data.is_user_auth(flask_security.current_user):
        return flask.abort(403)

    df = datatable.get_data()

    if flask.request.method == 'POST':
        op = flask.request.form.get('opinfo', '')
        column_name = flask.request.form.get('colinfo', '')

        if op == 'findreplace':
            find = flask.request.form.get('Find', '')
            replace = flask.request.form.get('Replace', '')
            df = transform.find_replace(df, column_name, find, replace)

        elif op == 'findreplacere':
            find = flask.request.form.get('Find', '')
            replace = flask.request.form.get('Replace', '')
            df = transform.find_replace_regex(df, column_name, find, replace)

        elif op == 'remove_outliers':
            outside_range = np.float64(flask.request.form.get('range', ''))
            df = transform.remove_outliers(df, column_name, outside_range)

        elif op == 'normalize':
            df = transform.normalize(df, column_name)

        elif op == 'fill_empty':
            replace_how = flask.request.form.get('radio', '')
            if replace_how == 'mean':
                df = transform.fill_empty_mean(df, column_name)
            elif replace_how == 'median':
                df = transform.fill_empty_median(df, column_name)
            elif replace_how == 'value':
                replace_with = flask.request.form.get('replace_with', '')
                df = transform.fill_empty_value(df, column_name, replace_with)

        elif op == 'discretize':
            nr_bins = int(flask.request.form.get('nr_bins', ''))
            boundaries = []
            for boundary in range(1, nr_bins):
                boundaries.append(float(flask.request.form.get('input_' + str(boundary), '')))
            df = transform.discretize_ranges(df, column_name, boundaries)

        elif op == 'discr_equiwidth':
            nr_bins = int(flask.request.form.get('nr_bins', ''))
            df = transform.discretize_equiwidth(df, column_name, nr_bins)

        elif op == 'discr_equifreq':
            nr_bins = int(flask.request.form.get('nr_bins', ''))
            df = transform.discretize_equifreq(df, column_name, nr_bins)

        elif op == 'one_hot_encode':
            use_old_name = flask.request.form.get('use_old', '') == 'on'
            df = transform.one_hot_encode(df, column_name, use_old_name)

        elif op == 'change_type':
            new_type = flask.request.form.get('radio', '')
            df = transform.change_type(df, column_name, new_type)

        elif op == 'parse_datetime':
            operation = flask.request.form.get('radio', '')
            df = transform.extract_from_datetime(df, column_name, operation[5: len(operation)])

        elif op == 'deduplication':
            threshold = int(flask.request.form.get('edit_distance', ''))
            duplicates = transform.find_duplicates(df, column_name, threshold)

            graph_data = []
            for column in datatable.columns.all():
                raw_data = datatable.get_data_raw()
                if pandas.api.types.is_numeric_dtype(raw_data[str(column.id)]):
                    graph_data.append([str(x) for x in transform.discretize_equiwidth(raw_data, str(column.id), 15)[
                        str(column.id)].value_counts().tolist()])
                else:
                    graph_data.append(["1" for x in range(15)])

            return flask.render_template('./Tables/index.html',
                                         cols=df.columns.values.tolist(),
                                         _dbname=db_id,
                                         duplicates=duplicates, col=column_name,
                                         logged=flask_security.current_user.is_authenticated,
                                         table_id=datatable.id)

        elif op == 'dedupe_result':
            nr_strings = int(flask.request.form.get('nr_strings', '0'))
            chain = flask.request.form.get('chain', '') == 'on'
            duplicates: {str, str} = {}

            for i in range(1, nr_strings + 1):
                if flask.request.form.get('replace_' + str(i), '') == 'on':
                    string: str = flask.request.form.get('string_' + str(i), '')
                    replacement: str = flask.request.form.get('replacement_' + str(i), '')
                    duplicates[string] = replacement
            df = transform.replace_duplicates(df, column_name, duplicates, chain)

        else:
            return flask.redirect(flask.request.url)

        datatable.load_data(df)
        return flask.redirect(flask.request.url)

    elif flask.request.method == 'GET':
        graph_data = []
        for column in datatable.columns.all():
            raw_data = datatable.get_data_raw()
            if pandas.api.types.is_numeric_dtype(raw_data[str(column.id)]):
                graph_data.append([str(x) for x in transform.discretize_equiwidth(raw_data, str(column.id), 15)[str(column.id)].value_counts().tolist()])
            else:
                graph_data.append(["1" for x in range(15)])
            # print(graph_data[len(graph_data) - 1])

        # Get the 5 (or less) last entries in the history
        versions = table_data.versions.all()
        history = [versions[entry - 1].description for entry in range(len(versions), max(0, len(versions) - 5), -1)]

        return flask.render_template('./Tables/index.html',
                                     cols=df.columns.values.tolist(),
                                     graph_data=graph_data,
                                     _dbname=db_id,
                                     logged=flask_security.current_user.is_authenticated,
                                     table_id=datatable.id,
                                     history=history)


@_app.route('/tables')
@flask_security.login_required
def tables_base():
    return "FAULTY"


@_app.route('/settings')
@flask_security.login_required
def settings_base():
    return flask.render_template('./Settings/index.html',
                                 user=flask_security.current_user,
                                 admin=flask_security.current_user.has_role(database.Role.query.get(0)),
                                 users=database.User.query.all(),
                                 logged=flask_security.current_user.is_authenticated)


@_app.route('/contact')
def contact_base():
    return flask.render_template('./Home/contact.html',
                                 logged=flask_security.current_user.is_authenticated)


@_app.route("/join/<data_id>/")
def join(data_id):
    data: database.Data = database.Data.query.get(data_id)

    if not data.is_user_auth(flask_security.current_user):
        flask.abort(403)

    version = data.get_latest_version()
    if version is None:
        flask.abort(500)

    tables = version.tables.all()

    column_list = {}
    table_list = {}
    for table in tables:
        table_list[table.name] = table.id
        column_list[table.id] = [column.name for column in table.columns.all()]

    return flask.render_template("Databases/JoinTables.html",
                                 tables=table_list, columns=column_list, _dbname=str(data_id),
                                 logged=flask_security.current_user.is_authenticated)


@_app.route('/databases/my_db')
@flask_security.login_required
def database_ext():

    usr = database.User.query.filter_by(username=flask_security.current_user.username).first()

    return flask.render_template('./Databases/MyDatabases.html', user=usr)


@_app.route('/database/upload', methods=['GET', 'POST'])
@flask_security.login_required
def upload_base():
    if flask.request.method == 'GET':
        return flask.render_template('./Databases/Upload.html')

    elif flask.request.method == 'POST':

        name = flask.request.form.get('inputName', '')
        desc = flask.request.form.get('inputDesc', '')

        if 'file' not in flask.request.files:
            flask.flash('No file part')
            print('No file part')
            return flask.redirect(flask.request.url)

        file = flask.request.files['file']

        def allowed_file(filename):
            return '.' in filename and \
                   filename.rsplit('.', 1)[1].lower() in _app.config['ALLOWED_EXTENSIONS']

        if file and allowed_file(file.filename):

            usr = database.User.query.filter_by(username=flask_security.current_user.username).first()

            filename = werkzeug.utils.secure_filename(file.filename)
            file.save(os.path.join(_app.config['UPLOAD_FOLDER'], filename))
            link = _app.config['UPLOAD_FOLDER'] + "/" + filename

            database.Data(user=usr, name=name, description=desc, in_file=link)

        return flask.redirect(flask.request.url)


@_app.route("/database/download/<data_id>/")
def download(data_id):
    data: database.Data = database.Data.query.get(data_id)

    if data is None:
        flask.abort(400)
    elif not data.is_user_auth(flask_security.current_user):
        flask.abort(403)

    kwargs = {}
    for option in flask.request.args:
        kwargs[option] = flask.request.args[option]

    table: database.DataTable = data.get_latest_version().tables.first()
    try:
        table.save(None, **kwargs)
    except Exception:
        flask.abort(500)

    return flask.send_file(table.file_name())


# Setup
# ------------------------------------------------------------------------------

# Push the app context
_app.app_context().push()
# Register the REST API
_app.register_blueprint(rest_api.restful_blueprint)
# Init the database and security
database.init_app(_app)
security.init_app(_app)


# WSGI support
# ------------------------------------------------------------------------------

_WSGI_APP = _app


# Running
# ------------------------------------------------------------------------------

if __name__ == '__main__':
    _app.run(debug=True)
