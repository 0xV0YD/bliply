from flask import Flask
from api.v1.optimizer_routes import optimizer_bp

app = Flask(__name__)
app.register_blueprint(optimizer_bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=6969, debug=True)
