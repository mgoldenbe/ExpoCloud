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
import traceback
from typing import Tuple

from src.constants import Constants
from multiprocessing import Queue
from multiprocessing.managers import SyncManager

class InstanceRole:
    PRIMARY_SERVER = 'PRIMARY_SERVER'
    BACKUP_SERVER = 'BACKUP_SERVER'
    CLIENT = 'CLIENT'

class MessageType:
    HEALTH_UPDATE = 'HEALTH_UPDATE'
    
    # to server
    REQUEST_TASKS = 'REQUEST_TASKS'
    STARTED = 'STARTED'
    RESULT = 'RESULT'
    REPORT_HARD_TASK = 'REPORT_HARD_TASK'
    LOG = 'LOG'
    EXCEPTION = 'EXCEPTION'
    BYE = 'BYE'

    # from primary to backup server
    NEW_CLIENT = 'NEW_CLIENT'
    CLIENT_FAILURE = 'CLIENT_FAILURE'
    MESSAGE_FROM_CLIENT = 'MESSAGE_FROM_CLIENT'

    # to client
    GRANT_TASKS = 'GRANT_TASKS'
    APPLY_DOMINO_EFFECT = 'APPLY_DOMINO_EFFECT'
    NO_FURTHER_TASKS = 'NO_FURTHER_TASKS'
    STOP = 'STOP'
    RESUME = 'RESUME'
    SWAP_QUEUES = 'SWAP_QUEUES'

    # from worker
    WORKER_STARTED = 'STARTED'
    WORKER_DONE = 'DONE'

# Adapted from https://stackoverflow.com/a/1365284/2725810
def get_unused_port():
    with socket.socket() as s:
        s.bind(('',0))
        return s.getsockname()[1]

instance_id = 0
def next_instance_name(role, prefix):
    global instance_id 
    instance_id += 1
    first_dash = '-' if prefix else ''
    if role == InstanceRole.CLIENT: 
        return f"{prefix}{first_dash}client-{instance_id}"
    assert(role == InstanceRole.BACKUP_SERVER)
    return f"{prefix}-server-{instance_id}"

def get_guest_qs(ip, port, q_names):
    """
    Get queues owned by another instance. The caller should handle the exceptions.
    """
    class MyManager(SyncManager):
        pass
    
    for q_name in q_names: MyManager.register(q_name)

    auth = b'myauth'
    manager = MyManager(address=(ip, port), authkey=auth)
    manager.connect()
    return tuple(getattr(manager, q_name)() for q_name in q_names)

def make_manager(q_names, port):
    class MyManager(SyncManager):
        pass
    for q_name in q_names:
        q = Queue()
        MyManager.register(q_name, callable=lambda q=q: q)

    auth = b'myauth'
    try:
        manager = MyManager(address=('', port), authkey=auth)
        manager.start()
    except Exception as e:
        handle_exception(e, 'Could not start manager')

    return manager

def handshake(my_role, my_port):
    try:
        server_ip = sys.argv[1]
        server_port = int(sys.argv[2])
        my_name = sys.argv[3]
    except Exception as e:
        handle_exception(e, f"Wrong command-line arguments {sys.argv}")

    try:
        handshake_q, = get_guest_qs(
            server_ip, server_port, ['handshake_q'])
        handshake_q.put((my_role, my_name, my_port))
    except Exception as e:
        handle_exception(e, 'Handshake with the server failed')

def handle_exception(e: Exception, msg: str, exit_flag: bool = True,
                     to_primary_q = None):
    """
    Print the custom error message and the exception and exit unless exit_flag==False.
    """
    descr = msg
    e_str = traceback.format_exc()
    if e_str: descr += "\n" + e_str
    if to_primary_q:
        to_primary_q.put((MessageType.EXCEPTION, descr))
    else:
        print(descr, file=sys.stderr, flush=True)
    if exit_flag: exit(1)

# Adapted from https://stackoverflow.com/a/53465812/2725810
def get_project_root() -> Path:
    return Path(__file__).parent.parent

def my_ip():
    return socket.gethostbyname(socket.gethostname())

def remote(ip, command_or_folder, type):
    key = '~/.ssh/id_rsa'
    if type == 'execute':
        command = command_or_folder
        ssh_command = \
                f"ssh {ip} -i {key} -o StrictHostKeyChecking=no \"{command}\" 2>>ssh_err"
    else:
        assert(type == 'copy')
        folder = command_or_folder
        ssh_command = \
                f"scp -i {key} -o StrictHostKeyChecking=no -r {folder} {ip}:{folder} 2>> ssh_err"
    print(ssh_command, flush=True)
    attempts_left = 3
    while attempts_left:
        try:
            status = subprocess.check_output(ssh_command, shell=True)
            return 0
        except Exception as e:
            attempts_left -= 1
            time.sleep(Constants.SSH_RETRY_DELAY)

    print(f"Failed to execute command remotely at {ip}", 
          file = sys.stderr, flush = True)
    return None

def remote_execute(ip, command):
    return remote(ip, command, 'execute')

def remote_replace(ip, folder):
    remote_execute(ip, f"rm -rf {folder}")
    return remote(ip, folder, 'copy')

def filter_indices(arr, cond):
    """
    Filter elements of arr, so only indices satisfying the predicate `cond` remain.
    """
    return [el[1]  for el in filter(lambda el: cond(el[0]), enumerate(arr))]

def list2str(assignment, sep = ';'):
    """
    Convert list to str using `sep` as separator
    """
    return sep.join([str(a) for a in assignment])

def set2str(s, sep = ';'):
    """
    Convert set to string using `sep` as separator. The elements are sorted.
    """
    return list2str(sorted(list(s)), sep)

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