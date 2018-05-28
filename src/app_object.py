import flask
import config

# The main flask app that will run the whole show
flask_app: flask.Flask = flask.Flask(__name__)
# Load the configuration from the Config class
flask_app.config.from_object(config.LocalConfig)
