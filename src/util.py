"""
Utility enums and functions.
"""

try:
    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials
except:
    pass

import os
from enum import Enum
from sre_parse import Verbose
import subprocess
import sys
import time
import socket
import queue
from pathlib import Path
import traceback
from typing import Tuple, List, Union, Callable

from src.constants import Constants, Verbosity
from multiprocessing import Queue
from multiprocessing.managers import SyncManager

def short_timestamp(timestamp: float) -> str:
    """
    Generate short string representation of the given timestamp.

    :param timestamp: The timestamp to render.
    :type timestamp: float
    :return: The short string representation of the given timestamp.
    :rtype: str
    """    
    return f"{timestamp % 10000 : .3f}"
    
def short_now_str() -> str:
    """
    Generate short string representation of the current timestamp.

    :return: The short string representation of the current timestamp.
    :rtype: str
    """
    return short_timestamp(time.time())

def my_print(cond: bool, str: str, err_flag: bool = False):
    """
    Print the given string if the given condition is ``True``.

    :param cond: The condition to be checked.
    :type cond: bool
    :param str: The string to be printed.
    :type str: str
    :param err_flag: ``True`` to print to ``stderr`` and ``False`` to print to ``stdout``; defaults to ``False``.
    :type err_flag: bool, optional
    """    
    file = sys.stderr if err_flag else sys.stdout
    if cond: 
        print(f"{short_now_str()}   {str}", file=file, flush=True)

def my_eprint(cond: bool, str: str):
    """
    Print the given string to ``stderr`` if the given condition is ``True``.

    :param cond: The condition to be checked.
    :type cond: bool
    :param str: The string to be printed.
    :type str: str
    """
    my_print(cond, str, True)

def command_arg_ip() -> str:
    """
    Get the command line argument representing the ip of the primary server.

    :return: The command line argument representing the ip of the primary server.
    :rtype: str
    """    
    try:
        return sys.argv[1]
    except Exception as e:
        handle_exception(e, f"Wrong command-line arguments")

def command_arg_port() -> int:
    """
    Get the command line argument representing the port for communicating with the primary server.

    :return: The command line argument representing the port for communicating with the primary server.
    :rtype: int
    """    
    try:
        return int(sys.argv[2])
    except Exception as e:
        handle_exception(e, f"Wrong command-line arguments")

def command_arg_name() -> str:
    """
    Get the command line argument representing the name of the current instance.

    :return: The command line argument representing the name of the current instance.
    :rtype: str
    """   
    try:
        return sys.argv[3]
    except Exception as e:
        handle_exception(e, f"Wrong command-line arguments")

def command_arg_max_cpus() -> int:
    """
    Get the command line argument representing the maximal number of workers that the current client instance is allowed to use. If it is ``None``, return a large positive integer value standing for infinity.

    :return: The maximal number of workers that the current client instance is allowed to use.
    :rtype: int
    """
    try:
        if sys.argv[4] == "None": return sys.maxsize
        return int(sys.argv[4])
    except Exception as e:
        handle_exception(e, f"Wrong command-line arguments")

class InstanceRole(Enum):
    """
    The role of instance.
    """

    PRIMARY_SERVER = 'PRIMARY_SERVER'
    """
    The primary server instance.
    """

    BACKUP_SERVER = 'BACKUP_SERVER'
    """
    The backup server instance.
    """

    CLIENT = 'CLIENT'
    """
    A client instance.
    """

class MessageType(Enum):
    """
    Type of a message sent to another instance.
    """

    HEALTH_UPDATE = 'HEALTH_UPDATE'
    """
    Informs the receiver that the sending instance is healthy.
    """    
    
    # to server
    REQUEST_TASKS = 'REQUEST_TASKS'
    """
    A client sends this message to the servers to request tasks for execution.
    """
    
    RESULT = 'RESULT'
    """
    A client sends this message to the servers to report on a completed task.
    """

    REPORT_HARD_TASK = 'REPORT_HARD_TASK'
    """
    A client sends this message to the servers to report on a timed out task.
    """

    LOG = 'LOG'
    """
    A client sends this message to the servers to report an even related to task execution.
    """

    EXCEPTION = 'EXCEPTION'
    """
    A client sends this message to the servers to report an exception event.
    """

    BYE = 'BYE'
    """
    A client sends this message to the servers to report that the client script is about to terminate.
    """

    # from primary to backup server
    NEW_CLIENT = 'NEW_CLIENT'
    """
    The primary server sends this message to the backup server to report that a new client has shaken hands with the primary server.
    """

    CLIENT_TERMINATED = 'CLIENT_TERMINATED'
    """
    The primary server sends this message to the backup server to report that a a client has been terminated.
    """

    MESSAGE_FROM_CLIENT = 'MESSAGE_FROM_CLIENT'
    """
    The primary server sends this message to forward to the backup server a message sent by a client.
    """

    # to client
    GRANT_TASKS = 'GRANT_TASKS'
    """
    A server sends this message to a client to assign to the latter tasks for execution.
    """

    APPLY_DOMINO_EFFECT = 'APPLY_DOMINO_EFFECT'
    """
    A server sends this message to a client to inform the latter of a task that timed out at another client.
    """

    NO_FURTHER_TASKS = 'NO_FURTHER_TASKS'
    """
    A server sends this message to a client, in response to requesting tasks for execution, to inform the client that there are no more tasks to be assigned.
    """

    STOP = 'STOP'
    """
    A server sends this message to a client to inform the latter that it should not send to the servers any messages aside from health updates.
    """

    RESUME = 'RESUME'
    """
    A server sends this message to a client to inform the latter that it may resume sending messages to the servers, thereby cancelling the effect of the previous ``STOP`` message.
    """

    SWAP_QUEUES = 'SWAP_QUEUES'
    """
    The primary server sends this message to a client to inform the latter that it should swap the queues for communicating with the primary and the backup server. This happens when the backup server assumes the primary server role following the failure of the former primary server.
    """

    # from worker
    WORKER_STARTED = 'STARTED'
    """
    A worker sends this message to its owning client to inform the latter that the worker has started executing the task.
    """

    WORKER_DONE = 'DONE'
    """
    A worker sends this message to its owning client to inform the latter that the worker has completed executing the task.
    """

# Adapted from https://stackoverflow.com/a/1365284/2725810
def get_unused_port() -> int:
    """
    Generate a yet unused port number.

    :return: An unused port number.
    :rtype: int
    """    
    with socket.socket() as s:
        s.bind(('',0))
        return s.getsockname()[1]

def extended_prefix(role: InstanceRole, prefix: str) -> str:
    """
    Compute the prefix for the name for the next cloud instance based on the role.

    :param role: The role of the new instance.
    :type role: InstanceRole
    :param prefix: The prefix to be used in the name.
    :type prefix: str
    :return: The prefix for the name for the next cloud instance based on the role.
    :rtype: str
    """
    first_dash = '-' if prefix else ''
    extension = 'client' if role == InstanceRole.CLIENT else 'server'
    return f"{prefix}{first_dash}{extension}"

def next_instance_name(
    role: InstanceRole, prefix: str, instance_id: dict) -> str:
    """
    Generate the name for the next cloud instance.

    :param role: The role of the new instance.
    :type role: InstanceRole
    :param prefix: The prefix to be used in the name.
    :type prefix: str
    :param instance_id: The dictionary that, maps the role to the id for the next instance.
    :type instance_id: dict
    :return: The name for the next cloud instance.
    :rtype: str
    """    
    if role not in instance_id: instance_id[role] = 0
    instance_id[role] += 1
    id = instance_id[role]
    return f"{extended_prefix(role, prefix)}-{id}"

def get_guest_qs(ip: str, port: int, q_names: List[str])->tuple[queue.Queue]:
    """
    Get queues owned by another instance, which we call here *guest*. The caller should handle the exceptions.

    :param ip: The IP address of the guest instance.
    :type ip: str
    :param port: The port for connecting to the guest instance.
    :type port: int
    :param q_names: The names of the queues to be obtained.
    :type q_names: List[str]
    :return: The requested queues owned by the guest instance.
    :rtype: tuple[queue.Queue]
    """ 
    class MyManager(SyncManager):
        pass
    
    for q_name in q_names: MyManager.register(q_name)

    auth = b'myauth'
    manager = MyManager(address=(ip, port), authkey=auth)
    manager.connect()
    return tuple(getattr(manager, q_name)() for q_name in q_names)

def make_manager(q_names: List[str], port:int) -> SyncManager:
    """
    Create a manager with the queues with the specified names.

    :param port: The port at which to accept connections to the new queues.
    :type port: int
    :param q_names: The names of the queues to be constructed.
    :type q_names: List[str]
    :return: The newly created manager.
    :rtype: SyncManager
    """ 
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

def handshake(my_role: InstanceRole, my_port1: int, my_port2: int = None):
    """
    Perform handshake with the primary server.

    :param my_role: The role of the current instance.
    :type my_role: InstanceRole
    :param my_port1: The port at which messages from the primary server will be received.
    :type my_port1: int
    :param my_port2: The port at which messages from the backup server will be received. This argument will be ``None`` when invoked at the backup server; defaults to ``None``.
    :type my_port2: int, optional
    """    
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
                     to_primary_q: queue.Queue = None):
    """
    Print the custom error message and the exception, optionally send an exception event to the primary server, and, again optionally, exit.

    :param e: The exception.
    :type e: Exception
    :param msg: The custom error message.
    :type msg: str
    :param exit_flag: Whether the script should terminate; defaults to ``True``.
    :type exit_flag: bool, optional
    :param to_primary_q: The queue for communication with the primary server or ``None``; defaults to ``None``.
    :type to_primary_q: queue.Queue, optional
    """                     
    descr = str(time.time()) + "   " + msg
    e_str = traceback.format_exc()
    if e_str: descr += "\n" + e_str

    my_eprint(Verbosity.all, descr)
    if to_primary_q:
        try:
            to_primary_q.put((MessageType.EXCEPTION, descr))
        except:
            pass

    if exit_flag: exit(1)

# Adapted from https://stackoverflow.com/a/53465812/2725810
def get_project_root() -> Path:
    """
    Return the root folder of ExpoCloud.

    :return: The root folder of ExpoCloud.
    :rtype: Path
    """    
    return Path(__file__).parent.parent

def my_name() -> str:
    """
    Return the name of the current instance.

    :return: The name of the current instance.
    :rtype: str
    """    
    return socket.gethostname()

def my_ip() -> str:
    """
    Return the IP address of the current instance.

    :return: The IP address of the current instance.
    :rtype: str
    """  
    return socket.gethostbyname(my_name())

def output_folder(instance_name: str = None) -> str:
    """
    Return the output folder for the given instance.

    :param instance_name: The name of the instance or ``None`` for the current instance; defaults to ``None``.
    :type instance_name: str, optional
    :return: The output folder for the given instance.
    :rtype: str
    """    
    if not instance_name: instance_name = my_name()
    return f"output-{instance_name}"

def pickled_file_name(path: str) -> str:
    """
    Return the path including the file name for the pickled server object to be located at the given path.

    :param path: The path at which the pickled server object is to be located.
    :type path: str
    :return: The path including the file name for the pickled server object.
    :rtype: str
    """    
    return os.path.join(path, 'pickled')

def ssh_command(ip: str, command: str) -> str:
    """
    Construct the shell command for executing the given command remotely using ssh.

    :param ip: The IP address of the instance at which the command is to be executed.    
    :type ip: str
    :param command: The command to be executed remotely.
    :type command: str
    :return: The shell command for executing the given command remotely using ssh.
    :rtype: str
    """    
    key = '~/.ssh/id_rsa'
    return f"ssh {ip} -i {key} -o StrictHostKeyChecking=no \"{command}\" 2>>ssh_err"

def scp_command(ip: str, source_folder: str, dest_folder: str) -> str:
    """
    Construct the shell command for copying the given local folder to the given remote folder.

    :param ip: The IP address of the destination instance.
    :type ip: str
    :param source_folder: The local folder.
    :type source_folder: str
    :param dest_folder: The destination folder.
    :type dest_folder: str
    :return: The shell command for copying the given local folder to the given remote folder.
    :rtype: str
    """    
    key = '~/.ssh/id_rsa'
    return f"scp -i {key} -o StrictHostKeyChecking=no -r {source_folder} {ip}:{dest_folder} 2>> ssh_err"

def attempt_command(command: str, n_attempts: int = 3) -> Union[int, None]:
    """
    Perform the given number of attempts at executing the given shell command. As soon as an attempt succeeds (i.e. no exception is thrown), return 0. If no attempt succeeds, return ``None``.

    :param command: The shell command to be executed.
    :type command: str
    :param n_attempts: The number of attempts, defaults to 3.
    :type n_attempts: int, optional
    :return: The status, either 0 or ``None``.
    :rtype: Union[int, None]
    """    
    attempts_left = 3
    while attempts_left:
        try:
            status = subprocess.check_output(command, shell=True)
            return 0
        except Exception as e:
            attempts_left -= 1
            time.sleep(Constants.SSH_RETRY_DELAY)

    my_print(Verbosity.command_lines, f"Failed to execute {command}")
    return None

def remote_execute(ip: str, command: str) -> Union[int, None]:
    """
    Remotely execute the given command. Uses :any:`attempt_command` to make several attempts.

    :param ip: The IP address of the instance at which the command is to be executed.
    :type ip: str
    :param command: The command to be executed remotely.
    :type command: str
    :return: The value returned by :any:`attempt_command`.
    :rtype: Union[int, None]
    """    
    return attempt_command(ssh_command(ip, command))

def remote_replace(ip, source_folder, dest_folder) -> Union[int, None]:
    """
    Copy the given local folder to the given remote folder. Uses :any:`attempt_command` to make several attempts.
    

    :param ip: The IP address of the destination instance.
    :type ip: str
    :param source_folder: The local folder.
    :type source_folder: str
    :param dest_folder: The destination folder.
    :type dest_folder: str
    :return: The value returned by :any:`attempt_command`.
    :rtype: Union[int, None]
    """  
    remote_execute(ip, f"rm -rf {dest_folder}")
    return attempt_command(scp_command(ip, source_folder, dest_folder))

def filter_indices(arr: list, cond: Callable) -> list:
    """
    Filter elements of the given list, so only indices satisfying the given predicate remain.

    :param arr: The list to be filtered.
    :type arr: list
    :param cond: The callable taking a list element and returning a Boolean indicating whether the element should be present in the filtered list.
    :type cond: Callable
    :return: The filtered list.
    :rtype: list
    """    
    return [el[1]  for el in filter(lambda el: cond(el[0]), enumerate(arr))]

def list2str(assignment: list, sep = ';') -> str:
    """
    Convert list to string using the given separator.

    :param assignment: The list to be converted.
    :type assignment: list
    :param sep: The separator, defaults to ``';'``.
    :type sep: str, optional
    :return: The string representation of the list.
    :rtype: str
    """    
    return sep.join([str(a) for a in assignment])

def set2str(s: set, sep = ';') -> str:
    """
    Convert list to string using the given separator. The elements are sorted.

    :param s: The set to be converted.
    :type s: set
    :param sep: The separator, defaults to ``';'``.
    :type sep: str, optional
    :return: The string representation of the set.
    :rtype: str
    """
    return list2str(sorted(list(s)), sep)

def tuple_le(t1, t2):
    """
    Returns ``True`` if ``t1`` and ``t2`` are of same length and all elements of ``t1`` are smaller than or equal to the corresponding elements of ``t2``.

    :param t1: The first tuple being compared.
    :type t1: tuple
    :param t2: The second tuple being compared.
    :type t2: tuple
    :return: ``True`` if ``t1`` and ``t2`` are of same length and all elements of ``t1`` are strictly smaller than or equal to the corresponding elements of ``t2``; otherwise ``False``.
    :rtype: bool
    """
    if not t1 or not t2 or len(t1) != len(t2): return False
    return sum([el1 <= el2 for el1, el2 in zip(t1, t2)])==len(t1)

def tuple_lt(t1: tuple, t2: tuple) -> bool:
    """
    Returns ``True`` if ``t1`` and ``t2`` are of same length and all elements of ``t1`` are smaller than or equal to the corresponding elements of ``t2``, with at least one inequality being strict.

    :param t1: The first tuple being compared.
    :type t1: tuple
    :param t2: The second tuple being compared.
    :type t2: tuple
    :return: ``True`` if ``t1`` and ``t2`` are of same length and all elements of ``t1`` are smaller than or equal to the corresponding elements of ``t2``, with at least one inequality being strict; otherwise ``False``.
    :rtype: bool
    """    
    if not (t1 <= t2): return False
    return sum([el1 < el2 for el1, el2 in zip(t1, t2)]) > 0

def filter_out(t1: Tuple, t2: Tuple) -> Tuple:
    """
    Return the tuple of elements in ``t1`` that are not in ``t2``.

    :param t1: The tuple being filtered.
    :type t1: Tuple
    :param t2: The tuple being subtracted.
    :type t2: Tuple
    :return: The tuple of elements in ``t1`` that are not in ``t2``.
    :rtype: Tuple
    """    
    return tuple(filter(lambda el: el not in t2, t1))

def tuple_to_csv(t: Tuple) -> str:
    """
    Return the string of comma-separated values based on the given tuple.
    :param t: The given tuple.
    :type t1: Tuple
    :return: The string of comma-separated values based on the given tuple.
    :rtype: str
    """
    return ",".join([str(el) for el in t])