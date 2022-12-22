from flask import Flask, request
from waitress import serve
from flask_cors import CORS

app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

def run_task(body_job_id, body_operator) :
    return "Success"

def is_valid_request(body_job_id, body_operator) :
    return True

@app.route("/task/<job_id>", methods=['POST'])
def task(job_id):
    body_job_id = job_id
    body_operator = job_id
    if is_valid_request(body_job_id, body_operator) == False :
        return "Bad request, invalid job_id: %s or operator: %s" % (body_job_id, body_operator), 401
    output = run_task(body_job_id, body_operator)
    return job_id, 201

@app.route('/', methods=['GET'])
def hello_world():
    return "Hello, World!"

if __name__ == '__main__':
    # app.run(host='0.0.0.0',debug=True, port=6060)
    serve(app, port=6060)
