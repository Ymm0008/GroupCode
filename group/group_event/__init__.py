# -*- coding: utf-8 -*-

from flask import Flask
# from flask_debugtoolbar import DebugToolbarExtension
from group_event.sensing.views import mod as group_event_sensingModule
from group_event.event_search.event_search import mod as event_searchModule
from group_event.geo_analysis.geo_views import mod as geo_analysis_Module
from group_event.network_analysis.views_network import mod as networkModule
from group_event.evolution_analysis.views_evolution import mod as risk_evolution_module

def create_app():
    app = Flask(__name__)
    
    # Create modules
    app.register_blueprint(geo_analysis_Module)
    app.register_blueprint(group_event_sensingModule)
    app.register_blueprint(event_searchModule)
    app.register_blueprint(networkModule)
    app.register_blueprint(risk_evolution_module)

    return app
