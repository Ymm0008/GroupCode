# -*- coding: utf-8 -*-

from flask import Flask
from flask_debugtoolbar import DebugToolbarExtension
from group_event.sensing.views import mod as group_event_sensingModule

def create_app():
    app = Flask(__name__)
    
    # Create modules

    app.register_blueprint(group_event_sensingModule)

    return app

