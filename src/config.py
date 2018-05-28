import os


class DB_URI:
    def __init__(self,
                 user:      str = '',
                 passwd:    str = '',
                 host:      str = '',
                 port:      str = '',
                 options:   list= []):
        self.uri = 'postgresql://{User}:{Pass}@{Host}:{Port}'.format\
                    (User=user, Pass=passwd, Host=host, Port=port)
        self.options = options

    def __str__(self):
        return self.uri + (lambda lst: '?'+lst[0]+'&'+'&'.join(lst[1:]) if len(lst) > 0 else '')(self.options)


class DB_URI_EXT(DB_URI):
    def __init__(self, uri: DB_URI, ext: str):
        self.uri = uri.uri + '/' + ext
        self.options = uri.options


class BaseConfig(object):
    DEBUG = False
    TESTING = False

    SECRET_KEY = os.environ.get('SECRET_KEY') or ''

    UPLOAD_FOLDER = '../Uploads'
    ALLOWED_EXTENSIONS = {'csv', "zip", "sql"}

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Disable csrf protection
    WTF_CSRF_ENABLED = False


class LocalConfig(BaseConfig):

    DB_USERNAME = ''
    DB_PASSWD = ''
    DB_HOST = ''
    DB_PORT = ''

    BASE_URI = DB_URI(user=DB_USERNAME, passwd=DB_PASSWD, host=DB_HOST, port=DB_PORT)

    SQLALCHEMY_DATABASE_URI = str(BASE_URI)

    GOOGLE_CLIENT_ID = ''
    GOOGLE_CLIENT_SECRET = ''

    SOCIAL_GOOGLE = {
        'consumer_key': '',
        'consumer_secret': ''
    }

    REDIRECT_URI = '/oauth2callback'


class ExtConfig(BaseConfig):

    DB_USERNAME = ''
    DB_PASSWD = ''
    DB_HOST = ''
    DB_PORT = ''

    BASE_URI = DB_URI(user=DB_USERNAME, passwd=DB_PASSWD, host=DB_HOST, port=DB_PORT)

    SQLALCHEMY_DATABASE_URI = str(BASE_URI)
