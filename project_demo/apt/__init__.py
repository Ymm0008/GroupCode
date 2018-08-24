# -*- coding: utf-8 -*-

from flask import Flask
from flask_debugtoolbar import DebugToolbarExtension
from apt.plot.views import mod as apt_searchModule

def create_app():
    app = Flask(__name__)
    # Create modules

    app.register_blueprint(apt_searchModule)

    return app

