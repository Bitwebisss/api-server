# app.py
from gevent import monkey
monkey.patch_all()


import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from server import app, socketio
import config

if __name__ == "__main__":
    socketio.run(app, host=config.HOST, port=config.PORT, debug=config.DEBUG)
