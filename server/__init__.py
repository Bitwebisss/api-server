# server/__init__.py
import logging
from flask import Flask, render_template
from flask_cors import CORS
from flask_socketio import SocketIO
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

app = Flask(__name__, template_folder="templates")
app.config["SECRET_KEY"] = config.SECRET
app.url_map.strict_slashes = False
CORS(app)

# async_mode=gevent — requires gevent.monkey.patch_all() before any imports (done in app.py)
# cors_allowed_origins="*" — HTTP CORS already handled by flask-cors above
socketio = SocketIO(app, async_mode="gevent", cors_allowed_origins="*")


@app.route("/")
def index():
    return render_template("index.html")


from server import rest   # noqa: E402 – must come after app + socketio are created
rest.init(app)
