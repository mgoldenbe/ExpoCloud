try:
    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials
except:
    pass

import sys
import time
import socket
from multiprocessing.managers import SyncManager

class MessageType:
    # to server
    REQUEST_TASKS = 'REQUEST_TASKS'
    BYE = 'BYE'
    REPORT_HARD_TASK = 'REPORT_HARD_TASK'

    # to client
    GRANT_TASKS = 'GRANT_TASKS'
    APPLY_DOMINO_EFFECT = 'APPLY_DOMINO_EFFECT'
    NO_FURTHER_TASKS = 'NO_FURTHER_TASKS'
    

def handle_exception(e: Exception, msg: str, exit_flag: bool = True):
    """
    Print the custom error message and the exception and exit unless exit_flag==False.
    """
    print(msg, file=sys.stderr, flush=True)
    print(e, file=sys.stderr, flush=True)
    if exit_flag: exit(1)
    
def my_ip():
    return socket.gethostbyname(socket.gethostname())

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

def tuple_to_csv(t):
    """
    Return comma-separated values based on tuple.
    """
    return ",".join([str(el) for el in t])

global_begin = time.time()
def print_event(descr, worker=None, task = None):
    worker_id = worker.id if worker else None
    task = task if task else worker.task
    print(f"{round(time.time()-global_begin, 2)},{descr},{worker_id},{task.id},"  + \
            tuple_to_csv(task.parameters()),
            file=sys.stderr, flush=True)

# https://cloud.google.com/compute/docs/reference/rest/v1/instances/stop
# Remember to give access to all APIs in the instance configuration
def stop_instance():
    try:
        credentials = GoogleCredentials.get_application_default()
        service = discovery.build('compute', 'v1', credentials=credentials)

        project = 'iucc-novel-heuristic'  # TODO: Update placeholder value.
        zone = 'us-central1-a'  # TODO: Update placeholder value.
        instance = 'test-00'  # TODO: Update placeholder value.

        request = service.instances().stop(project=project, zone=zone, instance=instance)
        request.execute()
    except:
        pass