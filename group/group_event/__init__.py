# -*- coding: utf-8 -*-

from flask import Flask
from flask_debugtoolbar import DebugToolbarExtension
from group_event.sensing.views import mod as group_event_sensingModule
from group_event.event_search.event_search import mod as event_searchModule

def create_app():
    app = Flask(__name__)
    
    # Create modules

    app.register_blueprint(group_event_sensingModule)
    app.register_blueprint(event_searchModule)

    return app

