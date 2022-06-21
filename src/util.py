try:
    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials
except:
    pass

import subprocess
import sys
import time
import socket
from pathlib import Path
from typing import Tuple

from src.constants import Constants

class MessageType:
    # to server
    HEALTH_UPDATE = 'HEALTH_UPDATE'
    REQUEST_TASKS = 'REQUEST_TASKS'
    RESULT = 'RESULT'
    REPORT_HARD_TASK = 'REPORT_HARD_TASK'
    LOG = 'LOG'
    EXCEPTION = 'EXCEPTION'
    BYE = 'BYE'

    # to client
    GRANT_TASKS = 'GRANT_TASKS'
    APPLY_DOMINO_EFFECT = 'APPLY_DOMINO_EFFECT'
    NO_FURTHER_TASKS = 'NO_FURTHER_TASKS'
    

def handle_exception(e: Exception, msg: str, exit_flag: bool = True,
                     to_server_q = None):
    """
    Print the custom error message and the exception and exit unless exit_flag==False.
    """
    descr = msg
    if str(e): descr += "\n" + str(e)
    if to_server_q:
        to_server_q.put((MessageType.EXCEPTION, descr))
    else:
        print(descr, file=sys.stderr, flush=True)
    if exit_flag: exit(1)

# Adapted from https://stackoverflow.com/a/53465812/2725810
def get_project_root() -> Path:
    return Path(__file__).parent.parent

def my_ip():
    return socket.gethostbyname(socket.gethostname())

def remote_execute(ip, command):
    key = '~/.ssh/id_rsa'
    ssh_command = \
            f"ssh {ip} -i {key} -o StrictHostKeyChecking=no \"{command}\""
    
    attempts_left = 3
    while attempts_left:
        try:
            status = subprocess.check_output(ssh_command, shell=True)
            return
        except Exception as e:
            attempts_left -= 1
            time.sleep(Constants.SSH_RETRY_DELAY)

    print(f"Failed to execute command remotely at {ip}", 
          file = sys.stderr, flush = True)
    return None

def all_lt(t1, t2):
    """
    Returns True if t1 and t2 are of same length and all elements of t1 are strictly smaller than the corresponding elements of t2.
    """
    if not t1 or not t2 or len(t1) != len(t2): return False
    return sum([el1 < el2 for el1, el2 in zip(t1, t2)])==len(t1)

def all_le(t1, t2):
    """
    Returns True if t1 and t2 are of same length and all elements of t1 are smaller or equal to the corresponding elements of t2.
    """
    return t1 == t2 or all_lt(t1, t2)

def filter_out(t1: Tuple, t2: Tuple):
    """
    Returns the tuple of elements in t1 that are not in t2.
    """
    return tuple(filter(lambda el: el not in t2, t1))

def tuple_to_csv(t):
    """
    Return comma-separated values based on tuple.
    """
    return ",".join([str(el) for el in t])

global_begin = time.time()

def output_event(to_server_q, descr, worker, task):
    """
    This function should not be invoked directly. Rather, use either `print_event` or `event_to_server`.
    """
    worker_id = worker.id if worker else None
    task = task if task else worker.task
    descr = f"{round(time.time()-global_begin, 2)},{descr},{worker_id},{task.id},"  + tuple_to_csv(task.parameters())
    if to_server_q:
        to_server_q.put((MessageType.LOG, descr))
    else:
        print(descr, file=sys.stderr, flush=True)

def print_event(descr, worker=None, task = None):
    output_event(None, descr, worker, task)
    
def event_to_server(to_server_q, descr, worker=None, task = None):
    output_event(to_server_q, descr, worker, task)