try:
    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials
except:
    pass

import os
from sre_parse import Verbose
import subprocess
import sys
import time
import socket
import queue
from pathlib import Path
import traceback
from typing import Tuple

from src.constants import Constants, Verbosity
from multiprocessing import Queue
from multiprocessing.managers import SyncManager

def short_timestamp(timestamp):
    return f"{timestamp % 10000 : .3f}"
    
def short_now_str():
    return short_timestamp(time.time())

def myprint(cond, str, err_flag = False):
    file = sys.stderr if err_flag else sys.stdout
    if cond: 
        print(f"{short_now_str()}   {str}", file=file, flush=True)

def myeprint(cond, str):
    myprint(cond, str, True)

def command_arg_ip():
    try:
        return sys.argv[1]
    except Exception as e:
        handle_exception(e, f"Wrong command-line arguments")

def command_arg_port():
    try:
        return int(sys.argv[2])
    except Exception as e:
        handle_exception(e, f"Wrong command-line arguments")

def command_arg_name():
    try:
        return sys.argv[3]
    except Exception as e:
        handle_exception(e, f"Wrong command-line arguments")

def command_arg_max_cpus():
    try:
        if sys.argv[4] == "None": return sys.maxsize
        return int(sys.argv[4])
    except Exception as e:
        handle_exception(e, f"Wrong command-line arguments")

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
    CLIENT_TERMINATED = 'CLIENT_TERMINATED'
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

def next_instance_name(role, prefix, instance_id):
    if role not in instance_id: instance_id[role] = 0
    instance_id[role] += 1
    id = instance_id[role]
    first_dash = '-' if prefix else ''
    if role == InstanceRole.CLIENT: 
        return f"{prefix}{first_dash}client-{id}"
    assert(role == InstanceRole.BACKUP_SERVER)
    return f"{prefix}{first_dash}server-{id}"

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
        q = queue.Queue()
        MyManager.register(q_name, callable=lambda q=q: q)

    auth = b'myauth'
    try:
        manager = MyManager(address=('', port), authkey=auth)
        manager.start()
    except Exception as e:
        handle_exception(e, 'Could not start manager')

    return manager

def handshake(my_role, my_port1, my_port2 = None):
    server_ip = command_arg_ip()
    server_port = command_arg_port()
    my_name = command_arg_name()

    try:
        handshake_q, = get_guest_qs(
            server_ip, server_port, ['handshake_q'])
        body = (my_role, my_name, my_port1)
        if my_port2: body += (my_port2,)
        handshake_q.put(body)
    except Exception as e:
        handle_exception(e, 'Handshake with the server failed')

def handle_exception(e: Exception, msg: str, exit_flag: bool = True,
                     to_primary_q = None):
    """
    Print the custom error message and the exception and exit unless exit_flag==False.
    """
    descr = str(time.time()) + "   " + msg
    e_str = traceback.format_exc()
    if e_str: descr += "\n" + e_str
    if to_primary_q:
        to_primary_q.put((MessageType.EXCEPTION, descr))
    else:
        myeprint(Verbosity.all, descr)
    if exit_flag: exit(1)

# Adapted from https://stackoverflow.com/a/53465812/2725810
def get_project_root() -> Path:
    return Path(__file__).parent.parent

def my_name():
    return socket.gethostname()

def my_ip():
    return socket.gethostbyname(my_name())

def output_folder(instance_name = None):
    if not instance_name: instance_name = my_name()
    return f"output-{instance_name}"

def pickled_file_name(path):
    return os.path.join(path, 'pickled')

def ssh_command(ip, command):
    key = '~/.ssh/id_rsa'
    return f"ssh {ip} -i {key} -o StrictHostKeyChecking=no \"{command}\" 2>>ssh_err"

def scp_command(ip, source_folder, dest_folder):
    key = '~/.ssh/id_rsa'
    return f"scp -i {key} -o StrictHostKeyChecking=no -r {source_folder} {ip}:{dest_folder} 2>> ssh_err"

def attempt_command(command, n_attempts = 3):
    attempts_left = 3
    while attempts_left:
        try:
            status = subprocess.check_output(command, shell=True)
            return 0
        except Exception as e:
            attempts_left -= 1
            time.sleep(Constants.SSH_RETRY_DELAY)

    myprint(Verbosity.command_lines, f"Failed to execute {command}")
    return None

def remote_execute(ip, command):
    return attempt_command(ssh_command(ip, command))

def remote_replace(ip, source_folder, dest_folder):
    remote_execute(ip, f"rm -rf {dest_folder}")
    return attempt_command(scp_command(ip, source_folder, dest_folder))

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