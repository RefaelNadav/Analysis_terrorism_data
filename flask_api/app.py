from flask import Flask

from flask_api.blue_prints.analysis_blueprint import analysis_blueprint

app = Flask(__name__)

app.register_blueprint(analysis_blueprint)

@app.route('/')
def hello_world():  # put application's code here
    return 'Hello World!'


if __name__ == '__main__':
    app.run()
