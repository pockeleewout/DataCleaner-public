import flask_security
import wtforms

import app_object
from database import db, User, Role

# Customized forms for Flask-Security
# ------------------------------------------------------------------------------


class CustomLoginForm(flask_security.LoginForm):
    """Custom form for login with username and not email"""
    email = wtforms.StringField("Username", [flask_security.forms.Required(message='EMAIL_NOT_PROVIDED')])


class CustomRegisterForm(flask_security.RegisterForm):
    username = wtforms.StringField(
        label='Username',
        description='Username...',
        validators=[flask_security.forms.validators.DataRequired()])

    first_name = wtforms.StringField(
        label='First name (optional)',
        description='First name...',
        validators=[flask_security.forms.validators.Optional()])

    last_name = wtforms.StringField(
        label='Last name (optional)',
        description='Last name...',
        validators=[flask_security.forms.validators.Optional()])

    email = wtforms.StringField(
        label='Email (optional)',
        description='Email...',
        validators=[flask_security.forms.validators.Optional(), flask_security.forms.email_validator])

    def validate(self):
        validation = super(CustomRegisterForm, self).validate()

        user_mail = User.query.filter_by(email=self.email.data).first()
        if user_mail is not None and user_mail.email != "":
            self.email.errors.append('An account with this email already exists.')
            return False

        user_name = User.query.filter_by(username=self.username.data).first()
        if user_name is not None:
            self.username.errors.append('An account with this username already exists.')
            return False

        return validation


# Set the app configuration for Flask-Security
app_object.flask_app.config["SECURITY_USER_IDENTITY_ATTRIBUTES"] = "username"
app_object.flask_app.config["SECURITY_REGISTERABLE"] = True
app_object.flask_app.config["SECURITY_TRACKABLE"] = True
app_object.flask_app.config["SECURITY_CHANGEABLE"] = True
app_object.flask_app.config["SECURITY_PASSWORD_HASH"] = "sha512_crypt"
app_object.flask_app.config["SECURITY_PASSWORD_SALT"] = "salt"
app_object.flask_app.config["SECURITY_SEND_REGISTER_EMAIL"] = False
app_object.flask_app.config["SECURITY_SEND_PASSWORD_CHANGE_EMAIL"] = False
app_object.flask_app.config["SECURITY_SEND_PASSWORD_RESET_EMAIL"] = False
app_object.flask_app.config["SECURITY_SEND_PASSWORD_RESET_NOTICE_EMAIL"] = False

# Set the datastore for Flask-Security
user_datastore = flask_security.SQLAlchemyUserDatastore(db, User, Role)
# Actually declare the security object, but do not register it to the app yet
security = flask_security.Security(datastore=user_datastore, login_form=CustomLoginForm,
                                   register_form=CustomRegisterForm)

def init_app(app, **kwargs):
    security.init_app(app, user_datastore, login_form=CustomLoginForm, register_form=CustomRegisterForm, **kwargs)
