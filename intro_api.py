from flask import Flask, request
from waitress import serve
from flask_cors import CORS
import time
import multiprocessing
from multiprocessing import Process

# http://0.0.0.0:6060/jobstatus/1234 - GET - to get job status
# http://0.0.0.0:6060/task/1234 - POST - to create a job

RUNNING_STATUS = "RUNNING"
WAITING_STATUS = "WAITING"
FAILED_STATUS = "FAILED"
COMPLETED_STATUS = "COMPLETED"

MAX_NUM_CONCURRENT_RUNNING_JOBS = 8
MAX_NUM_CONCURRENT_PROCESSING_JOBS = 4

JOB_NOT_FOUND = "JOB NOT FOUND"

SLEEPING_SECONDS = 5
LONG_SLEEPING_SECONDS = 30

jobs = {}
app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*"}})

def update_job_status(jobs, job_id, status):
    jobs[job_id] = status
    print(jobs)
    

def get_num_jobs(status):
    global jobs
    returnnumber = 0
    for i in jobs:
        if jobs[i] == status:
            returnnumber += 1
    return returnnumber

def get_num_processing_jobs():
    global jobs
    returnnumber = 0
    for i in jobs:
        if jobs[i] != COMPLETED_STATUS:
            returnnumber += 1
    return returnnumber

@app.route("/jobstatus/<job_id>")
def get_job_status(job_id):
    print(jobs)
    if job_id in jobs:
        return jobs[job_id]
    return JOB_NOT_FOUND

def run_task(job_id, jobs) :
    print("WWWWWWWWW")
    update_job_status(jobs, job_id, WAITING_STATUS)

    while get_num_jobs(RUNNING_STATUS) > MAX_NUM_CONCURRENT_RUNNING_JOBS:
        time.sleep(SLEEPING_SECONDS)

    update_job_status(jobs, job_id, RUNNING_STATUS)

    try:
        time.sleep(LONG_SLEEPING_SECONDS)
        update_job_status(jobs, job_id, COMPLETED_STATUS)
    except:
        update_job_status(jobs, job_id, FAILED_STATUS)
        raise Exception(403)

def is_valid_request(body_job_id) :
    return True

@app.route("/task/<job_id>", methods=['POST'])
def task(job_id):
    
    body_job_id = job_id
    if is_valid_request(body_job_id) == False :
        return "Bad request, invalid job_id: %s " % (body_job_id), 401
    
    number_process_running = get_num_processing_jobs()
    if number_process_running >= MAX_NUM_CONCURRENT_PROCESSING_JOBS:
        return "Bad request,  job_id: %s declined. Too many jobs processing : %s" % (body_job_id, number_process_running), 401
    
    task_cb = Process(target=run_task, args=(body_job_id,jobs))
    
    task_cb.start()
    
    ret = "Job received : %s", job_id
    return [ret, 202]

@app.route('/', methods=['GET'])
def hello_world():
    return "Hello, World!"

if __name__ == '__main__':
    manager = multiprocessing.Manager()
    jobs = manager.dict()
    
    # app.run(host='0.0.0.0',debug=True, port=6060)
    serve(app, port=6060)
