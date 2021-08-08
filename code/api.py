import json
import os

from flask import request, jsonify
from werkzeug.utils import secure_filename
from flask import Flask

from database.db_functions import DBConnection
from main import LoadData

UPLOAD_FOLDER = os.getcwd() + '/uploads'

app = Flask(__name__)
app.secret_key = "secret key"
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

ALLOWED_EXTENSIONS = {'csv'}


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def get_config():
    with open(os.getcwd() + 'code/database/config.json', 'r') as f:
        config = json.load(f)
    return config


@app.route('/file-upload', methods=['POST'])
def upload_file():
    config = get_config()
    d = DBConnection(config['postgres']['db'], config['postgres']['user'], config['postgres']['passwd'],
                     config['postgres']['host'])
    if 'file' not in request.files:
        resp = jsonify({'message': 'No file part in the request'})
        resp.status_code = 400
        return resp
    file = request.files['file']
    if file.filename == '':
        resp = jsonify({'message': 'No file selected for uploading'})
        resp.status_code = 400
        return resp
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        LoadData.preprocess_data(os.getcwd() + '/uploads/' + filename)
        status = d.upload_file(os.getcwd() + '/processed_data/preprocessed.csv')
        if status is None:
            resp = jsonify({'message': 'File successfully uploaded'})
            resp.status_code = 201
        else:
            resp = jsonify({'message': 'something went wrong'})
            resp.status_code = 400
        return resp
    else:
        resp = jsonify({'message': 'Allowed file is only csv'})
        resp.status_code = 400
        return resp


@app.route('/update', methods=['POST'])
def update_table():
    config = get_config()
    d = DBConnection(config['postgres']['db'], config['postgres']['user'], config['postgres']['passwd'],
                     config['postgres']['host'])
    if 'file' not in request.files:
        resp = jsonify({'message': 'No file part in the request'})
        resp.status_code = 400
        return resp
    file = request.files['file']
    if file.filename == '':
        resp = jsonify({'message': 'No file selected for uploading'})
        resp.status_code = 400
        return resp
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        status = d.update_table(LoadData.preprocess_data(os.getcwd() + '/uploads/' + filename))
        if status is None:
            resp = jsonify({'message': 'File successfully uploaded'})
            resp.status_code = 201
        else:
            resp = jsonify({'message': 'something went wrong'})
            resp.status_code = 400
        return resp
    else:
        resp = jsonify({'message': 'Allowed file is only csv'})
        resp.status_code = 400
        return resp


@app.route('/fetch_data', methods=['POST', 'GET'])
def fetch_data():
    config = get_config()
    d = DBConnection(config['postgres']['db'], config['postgres']['user'], config['postgres']['passwd'],
                     config['postgres']['host'])
    limit = None
    if request.method == 'GET':
        fetched_data = d.fetch_table_data(limit)
        return jsonify({'fetched data': fetched_data})
    if request.method == 'POST':
        limit = request.json['limit']
        fetched_data = d.fetch_table_data(limit)
        return jsonify({'fetched data': fetched_data})


if __name__ == "__main__":
    app.run()
