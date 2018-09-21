# -*- coding: utf-8 -*-

from flask import Flask
from flask_debugtoolbar import DebugToolbarExtension
from group_event.sensing.views import mod as group_event_sensingModule
from group_event.opinion_cluster.views_opinion_cluster import mod as group_event_networkModule
from group_event.opinion_cluster.views_opinion_cluster import mod as group_event_opinionModule

def create_app():
    app = Flask(__name__)
    
    # Create modules

    app.register_blueprint(group_event_networkModule)
    app.register_blueprint(group_event_sensingModule)
    app.register_blueprint(group_event_opinionModule)


    return app

