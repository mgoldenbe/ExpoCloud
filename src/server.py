"""
The class implementing the primary and the backup servers.
"""

import pickle
import subprocess
import time
import os
import sys
import traceback
from typing import Any, List, Union
from src.abstract_engine import AbstractEngine
from src.engines.local import LocalEngine

from src.util import my_print
from src.constants import Verbosity

from src import util
from src.abstract_task import AbstractTask
from src.util import InstanceRole, MessageType, handle_exception
from src.constants import Constants
from src.instance import ClientInstance, BackupServerInstance, Instance,PrimaryServerInstance, is_primary, is_backup, is_client

class Server():
    """
    Either the primary or the backup server.
    """
    
    def __init__(
        self, 
        tasks: List[AbstractTask], 
        engine: Union[LocalEngine, AbstractEngine], 
        backup: bool, 
        max_clients: int = None, 
        max_cpus_per_client: int = None, 
        min_group_size: int = 0):
        """
        The constructor; only ever invoked for building the first primary server.

        :param tasks: The list of tasks to be executed.
        :type tasks: List[AbstractTask]
        :param engine: The compute engine settings.
        :type engine: Union[LocalEngine, AbstractEngine]
        :param backup: Whether or not to use a backup server.
        :type backup: bool
        :param max_clients: Maximal number of client instances or ``None`` for no restriction; defaults to ``None``.
        :type max_clients: int, optional
        :param max_cpus_per_client: The maximal number of workers to be used at a client instance or ``None`` for no restriction; defaults to ``None``
        :type max_cpus_per_client: int, optional
        :param min_group_size: minimal size of group as defined by the task's :any:`group_parameter_titles<src.abstract_task.AbstractTask.group_parameter_titles>` method, defaults to 0
        :type min_group_size: int, optional        
        """
        
        my_print(Verbosity.all, "Constructing the server")
        self.role = InstanceRole.PRIMARY_SERVER
        self.port = util.get_unused_port()
        self.engine = engine
        self.backup = backup
        self.max_clients = max_clients
        self.max_cpus_per_client = max_cpus_per_client
        self.clients = []
        self.clients_stopped_timestamp = None

        self.init_handshake_q()

        # Store original order for results output, then sort by difficulty
        for i, t in enumerate(tasks): 
            t.orig_id = i
        self.tasks = sorted(tasks, key = lambda t: t.hardness)
        
        self.next_task = 0 # next task to be given to clients
        self.tasks_from_failed = [] # tasks from failed clients to reassign
        self.min_hard = [] # hardness for each minimally hard task
        self.min_group_size = min_group_size

        for i, t in enumerate(self.tasks):
            t.id = i
            t.result = None
        
        self.output_folder = util.output_folder()
        os.makedirs(self.output_folder, exist_ok=True)
        
        self.primary_server = None
        self.backup_server = None
        self.to_client_id = 1000 # id of the next outbound message to a client
        
    def __del__(self):
        """
        The destructor. If the server is primary, shuts the handshake manager.
        """        
        my_print(Verbosity.all, "Shutting down")
        if self.is_primary():
            self.handshake_manager.shutdown()

    def run(self):
        """
        The main loop.
        """        
        if self.is_primary():
            my_print(Verbosity.all, 
                    f"Got {len(self.tasks)} tasks and ready for clients")

        try:
            printed_results = False
            while True:
                self.send_health_update()
                if self.is_primary(): self.accept_handshakes()
                self.handle_messages()
                if self.is_primary(): self.create_instance()
                self.kill_unhealthy_instances()
                if not printed_results:
                    if not (self.tasks_remain() or self.clients):
                        self.print_results()
                        printed_results = True
                        if self.engine.is_local(): break
                time.sleep(Constants.SERVER_CYCLE_WAIT)
        except Exception as e:
            handle_exception(e, "Exception in Server.run")
        

#region TASKS

    def tasks_remain(self) -> bool:
        """
        Check whether there are tasks remaining to be executed.

        :return: ``True`` if there are tasks remaining to be executed and ``False`` otherwise.
        :rtype: bool
        """        
        return self.tasks_from_failed or self.next_task < len(self.tasks)

    def is_hard(self, hardness: tuple) -> bool:
        """
        Check whether :paramref:`hardness` is greater or equal to hardness of one of the timed out tasks.

        :param hardness: The hardness to be checked.
        :type hardness: tuple
        :return: ``True`` if `hardness` is greater or equal to hardness of one of the timed out tasks and ``False`` otherwise.
        :rtype: bool
        """        
        for h in self.min_hard:
            if hardness >= h: return True
        return False

    def print_results(self):
        """
        Restore the original order of tasks and print results.
        """
        my_print(Verbosity.all, "Printing results")
        group_counts = {}
        for t in self.tasks:
            group = t.group_parameters()
            if group not in group_counts: group_counts[group] = 0
            group_counts[group] += 1

        self.tasks.sort(key = lambda t: t.orig_id)

        results_file = \
            open(os.path.join(self.output_folder, 'results.txt'), "w")
        print(util.tuple_to_csv(self.tasks[0].parameter_titles() + \
                                self.tasks[0].result_titles()),
              file = results_file)
        for t in self.tasks:
            if not t.result: continue
            if group_counts[t.group_parameters()] >= self.min_group_size:
                print(util.tuple_to_csv(t.parameters() + t.result), 
                      file = results_file)
        
        results_file.close()
        my_print(Verbosity.all, "Done printing results")

#endregion TASKS

#region ROLES

    def is_primary(self) -> bool:
        """
        Check whether the server is the primary server.

        :return: ``True`` if the server is the primary server and ``False`` otherwise.
        :rtype: bool
        """        
        return self.role == InstanceRole.PRIMARY_SERVER

    def is_backup(self) -> bool:
        """
        Check whether the server is the backup server.

        :return: ``True`` if the server is the backup server and ``False`` otherwise.
        :rtype: bool
        """        
        return self.role == InstanceRole.BACKUP_SERVER

    def assume_backup_role(self):
        """
        Assume the backup server role.
        This method is called after unpickling the primary server object, so as to convert it to a backup server one.
        """
        assert(self.is_primary())
        self.role = InstanceRole.BACKUP_SERVER
        self.output_folder = util.output_folder(util.command_arg_name())
        self.backup_server = None

        self.port = util.get_unused_port()
        self.primary_server = PrimaryServerInstance(self.port)
        util.handshake(self.role, self.port)
        my_print(Verbosity.all, "Handshake with primary server complete")

        if len(self.clients) > 0 and not self.clients[-1].active_timestamp:
            self.clients = self.clients[:-1]
            
        for c in self.clients:
            c.shake_hands(self.role, self.output_folder)
            c.engine = None
            c.received_ids = []
    
    def assume_primary_role(self):
        """
        Assume the primary server role. This method is called at the backup server when primary server failure is detected.
        """
        assert(self.is_backup())
        self.role = InstanceRole.PRIMARY_SERVER
        self.primary_server = None
        self.backup_server = None
        self.init_handshake_q()
        for c in self.clients: c.engine = self.engine
        self.engine.kill_dangling_clients([c.name for c in self.clients])
    
    def handle_primary_server_failure(self):
        """
        Handle the primary server failure. This method is called at the backup server when primary server failure is detected. The handling consists of assuming the primary server role and sending the :any:`MessageType.SWAP_QUEUES<SWAP_QUEUES>` message to the clients.
        """        
        my_print(Verbosity.all, "Handling primary server failure")
        self.assume_primary_role()
        for c in self.clients:
            if not c.active_timestamp: continue
            temp = c.outbound_q
            try:
                c.outbound_q, = util.get_guest_qs(
                    c.ip, c.port_primary, ['inbound'])
            except:
                my_print(Verbosity.all, 
                        "Temporary connection to from_primary_q failed")
            self.message_to_instance(c, MessageType.SWAP_QUEUES, None)
            c.port_primary, c.port_backup = c.port_backup, c.port_primary
            c.outbound_q = temp
            c.engine = self.engine
#endregion ROLES

#region INSTANCES

    def init_handshake_q(self):
        """
        Construct the handshake manager and the queue for handshake requests from the backup server and the clients.
        """        
        self.handshake_manager = \
            util.make_manager(['handshake_q'], self.port)
        self.handshake_q = self.handshake_manager.handshake_q()

    def handshake_from_client(
        self, name: str, port_primary: int, port_backup: int):
        """
        Handle handshake request from a client. This method is invoked only at the primary server.

        :param name: The name of the client instance.
        :type name: str
        :param port_primary: The client's port for communication with the primary server.
        :type port_primary: int
        :param port_backup: The client's port for communication with the backup server.
        :type port_backup: int
        """        
        client = self.get_client(name)
        if not client:
            my_print(Verbosity.all, f"Unknown client {name} tried to connect")
            return
        client.port_primary, client.port_backup = port_primary, port_backup
        client.shake_hands(self.role, self.output_folder)

        # inform the backup server
        my_print(Verbosity.all, f"Informing backup server of {client.name}")
        self.message_to_instance(
            self.backup_server, MessageType.NEW_CLIENT, 
                (client.name, client.ip, 
                 port_primary, port_backup, client.active_timestamp))
    
    def handshake_from_backup(self, name: str, port: int):
        """
        Handle handshake request from the backup server. This method is invoked only at the primary server.

        :param name: The name of the backup server instance.
        :type name: str
        :param port: The backup server's port for communication with the primary server.
        :type port: int
        """
        if name != self.backup_server.name:
            my_print(Verbosity.all, 
                    f"Unknown backup server {name} tried to connect")
            return
        self.backup_server.port = port
        self.backup_server.shake_hands()
        self.resume_clients()

    def accept_handshakes(self):
        """
        Handle handshake requests from new backup server and client instances. Uses the methods :any:`handshake_from_client` and :any:`handshake_from_backup` to handle the respective kinds of requests. This method is invoked only at the primary server.
        """        
        while not self.handshake_q.empty():
            body = self.handshake_q.get_nowait()
            role, rest = body[0], body[1:]
            
            if role == InstanceRole.CLIENT:
                if self.clients_stopped_timestamp:
                    self.handshake_q.put(body)
                    continue
                name, port_primary, port_backup = rest
                self.handshake_from_client(name, port_primary, port_backup)
                continue

            if role == InstanceRole.BACKUP_SERVER:
                name, port = rest
                self.handshake_from_backup(name, port)
                continue

            assert(False)

    def n_active_clients(self) -> int:
        """
        Compute the number of currently active client instances. An instance is active if it has shaken hands with the server and has not been terminated since then.

        :return: The number of currently active client instances.
        :rtype: int
        """        
        return len(list(filter(lambda c: c.active_timestamp, self.clients)))

    def get_client(self, name: str) -> ClientInstance:
        """
        Get the object representing the client instance with the given name.

        :param name: The name of the client instance.
        :type name: str
        :return: The object representing the client instance or ``None`` if there is no client instance with the given name.
        :rtype: ClientInstance
        """        
        try:
            return next(filter(lambda c: c.name == name, self.clients))
        except:
            return None

    def kill_client(self, name: str):
        """
        Terminate the client instance with the given name. If the name does not correspond to any client, do nothing.

        :param name: The name of the client instance.
        :type name: str
        """
        self.clients = \
            list(filter(lambda c: c.name != name, self.clients))

    def kill_instance(self, instance: Instance):
        """
        Terminate the instance represented by :paramref:`instance`.

        :param instance: The object representing the instance to be terminated.
        :type name: Instance
        """
        if is_client(instance):
            self.kill_client(instance.name)
            return
        if is_backup(instance):
            self.backup_server = None
            return
        if is_primary(instance):
            self.handle_primary_server_failure()
            return
        assert(False)
    
    def create_backup_server_instance(self):
        """
        Create and run the backup server.
        """        
        def pickle_server():
            # exclude things that should not be pickled/unpickled
            temp_handshake_manager, temp_handhsake_q, temp_backup_server = \
                self.handshake_manager, self.handshake_q, self.backup_server
            self.handshake_manager, self.handshake_q, self.backup_server = \
                None, None, None
            client_files = []
            for c in self.clients:
                if not c.active_timestamp: continue
                client_files.append((c.events_file, c.exceptions_file))
                c.events_file, c.exceptions_file = None, None
            with open(util.pickled_file_name(self.output_folder), 'wb') as f:
                pickle.dump(self, f)
            self.handshake_manager, self.handshake_q, self.backup_server = \
                temp_handshake_manager, temp_handhsake_q, temp_backup_server
            for c in self.clients:
                if not c.active_timestamp: continue
                c.events_file, c.exceptions_file  = client_files.pop(0)
        
        def copy_output_folder():
            backup_output_folder = \
                    util.output_folder(self.backup_server.name)
            if not self.engine.is_local():
                util.remote_replace(
                    self.backup_server.ip, 
                    self.output_folder,
                    os.path.join(self.engine.root_folder, 
                                 backup_output_folder))
            else:
                command = f"cp -r {self.output_folder} {backup_output_folder}"
                subprocess.check_output(command, shell=True)

        # If not first backup server instance, make sure all clients got the STOP message and all client messages had been handled
        if self.clients_stopped_timestamp:
            if time.time() - self.clients_stopped_timestamp <= \
               Constants.CLIENTS_TIME_TO_STOP: 
               return
            for c in self.clients:
                if self.messages_waiting(c): return

        if not self.backup_server:
            self.backup_server = BackupServerInstance(self.engine)
            self.backup_server_has_been_run = False
            my_print(Verbosity.all, 
                    f"Created instance object for {self.backup_server.name}. Stopping clients...")
            self.stop_clients()
            return

        assert(self.backup_server)
        if not self.backup_server.ip:
            self.backup_server.create()
            return

        assert(self.backup_server.ip)
        if self.backup_server_has_been_run: return
        pickle_server()
        copy_output_folder()
        self.backup_server.run(self.port)
        self.backup_server_has_been_run = True

    def create_client_instance(self):
        """
        Create and run a new client.
        """
        # If no more tasks, don't create another client
        if not self.tasks_remain(): return

        # Make a client if all existing clients have ip
        if len(self.clients) == 0 or self.clients[-1].ip:
            client = ClientInstance(self.engine, self.tasks_from_failed)
            self.clients.append(client)
            my_print(Verbosity.all, f"Created instance object for {client.name}")
        client = self.clients[-1]
        if not client.ip: client.create()
        if client.ip: client.run(self.port, self.max_cpus_per_client)

    def create_instance(self):
        """
        Create and run the new instance. The backup server gets a precedence. If it already exists or is not used, then a new client is created.
        """
        if self.backup and \
            (not self.backup_server or not self.backup_server.active_timestamp):
            self.create_backup_server_instance()
        else:
            if (not self.max_clients) or \
               self.n_active_clients() < self.max_clients:
                self.create_client_instance()

    def kill_unhealthy_instances(self):
        """
        Terminate unhealthy instances. See :any:`Instance.is_healthy` and :any:`PrimaryServerInstance.is_healthy` for the definition of a healthy instance.
        """        
        if self.is_backup():
            if not self.primary_server.is_healthy():
                self.handle_primary_server_failure()     
            return
        
        assert(self.is_primary())
        tasks_remain = self.tasks_remain()
        for c in self.clients:
            if c.is_healthy(tasks_remain): continue
            self.kill_client(c.name)
            self.message_to_instance(
                self.backup_server, MessageType.CLIENT_TERMINATED, c.name)
        
        if self.backup_server and \
           not self.backup_server.is_healthy(tasks_remain):
            self.backup_server = None

    def stop_clients(self):
        """
        Send the :any:`MessageType.STOP` message to all clients and record the timestamp of stopping the clients.
        """        
        for c in self.clients:
            self.message_to_instance(c, MessageType.STOP, None)
        self.clients_stopped_timestamp = time.time()
    
    def resume_clients(self):
        """
        Send the :any:`MessageType.RESUME` message to all clients and reset the timestamp previously set by :any:`stop_clients`.
        """   
        for c in self.clients:
            self.message_to_instance(c, MessageType.RESUME, None)
        self.clients_stopped_timestamp = None

    def send_health_update(self):
        """
        Send health update to the other server.
        """
        other_server = \
            self.backup_server if self.is_primary() else self.primary_server
        self.message_to_instance(
            other_server, MessageType.HEALTH_UPDATE, None)

#endregion INSTANCES

#region MESSAGES
    def message_to_instance(
        self, instance: Instance, type:MessageType, body: Any):
        """
        Send a message to an instance.

        :param instance: The object representing the receiver instance.
        :type instance: Instance
        :param type: The message type.
        :type type: MessageType
        :param body: The body of the message.
        :type body: Any
        """        
        if not instance or not instance.active_timestamp:
            return

        try:
            message = (type, body)
            if is_client(instance) and \
               type not in [MessageType.STOP, MessageType.RESUME]:
                message = (self.to_client_id,) + message
                my_print(Verbosity.messages, 
                        f"Sending message {self.to_client_id} ({type}) to {instance.name}")
                self.to_client_id += 1
            else:
                message = (None,) + message
            instance.outbound_q.put(message)
        except:
            my_print(Verbosity.all, f"Sending to {instance.name} failed")
            my_print(Verbosity.failure_traceback, traceback.format_exc())
            self.kill_instance(instance)

    def messages_waiting(self, instance: Instance) -> bool:
        """
        Check whether a message from the given instance can be read.
        If the instance is invalid or is not active, returns ``False``.
        If there is no message in the inbound queue from the given instance, returns ``False``. Otherwise return ``True``, unless the method is invoked at the backup server, the instance is a client and the corresponding message has not been forwarded by the primary server.

        :param instance: The object representing the instance sending the message.
        :type instance: Instance
        :return: ``True`` if a message from the given instance can be read and ``False`` otherwise.
        :rtype: bool
        """        
        try:
            if not instance or not instance.active_timestamp: return False
            if instance.inbound_q.empty(): return False
            
            if self.is_primary(): return True

            assert(self.is_backup())
            if not is_client(instance): return True
            if not instance.received_ids: return False
            return True
        except:
            my_print(Verbosity.all, f"Listening to {instance.name} failed")
            my_print(Verbosity.failure_traceback, traceback.format_exc())
            self.kill_instance(instance)

    def forward_message(
        self, instance: Instance, 
        message_id: int, type: MessageType, body: Any):
        """
        Forward a message from a client to the backup server. This method is invoked only at the primary server.

        :param instance: The object representing the sending instance.
        :type instance: Instance
        :param message_id: The id of the message.
        :type message_id: int
        :param type: The message type.
        :type type: MessageType
        :param body: The body of the message.
        :type body: Any
        """
        if instance.role != InstanceRole.CLIENT: return
        self.message_to_instance(
            self.backup_server, MessageType.MESSAGE_FROM_CLIENT, 
            (instance.name, message_id, type, body))

#endregion MESSAGES

#region PROCESSING MESSAGES

    def process_health_update(self, instance: Instance, _body):
        """
        Record the timestamp of the :any:`MessageType.HEALTH_UPDATE` message sent by the given instance.

        :param instance: The object representing the instance sending the message.
        :type instance: Instance
        """        
        instance.active_timestamp = time.time()

    def process_request_tasks(self, client: ClientInstance, n: int):
        """
        Process the request for tasks sent by the given client. The method sends either the :any:`MessageType.GRANT_TASKS` message containing the tasks being granted to the client or the :any:`MessageType.NO_FURTHER_TASKS` message to indicate that there are no tasks to be assigned.

        :param client: The object representing the client sending the request.
        :type client: ClientInstance
        :param n: The number of tasks requested by the client.
        :type n: int
        """        
        tasks = []
        while n > 0 and self.tasks_remain():
            task = self.tasks[self.next_task]
            if self.tasks_from_failed:
                task = self.tasks[self.tasks_from_failed.pop(0)]
            else:
                self.next_task += 1
            if self.is_hard(task.hardness):
                my_print(Verbosity.all, f"Skipping hard task {task.id}")
                continue
            tasks.append(task)
            n -= 1
        if tasks:
            try:
                client.register_tasks(tasks)
                self.message_to_instance(
                    client, MessageType.GRANT_TASKS, tasks)
            except Exception as e:
                handle_exception(e, "Failed to send tasks")
        if n > 0:
            self.message_to_instance(
                client, MessageType.NO_FURTHER_TASKS, None)

    def process_log(self, client: ClientInstance, descr: str):
        """
        Log the event related to a task execution by the given client.

        :param client: The object representing the client sending the event.
        :type client: ClientInstance
        :param descr: The description of the event to be logged.
        :type descr: str
        """        
        print(descr, file=client.events_file, flush=True)

    def process_exception(self, client: ClientInstance, descr: str):
        """
        Log the exception event sent by the given client.

        :param client: The object representing the client sending the event.
        :type client: ClientInstance
        :param descr: The description of the event to be logged.
        :type descr: str
        """
        print(descr, file=client.exceptions_file, flush=True)

    def process_result(self, client: ClientInstance, body: tuple):
        """
        Process the result of executing a task sent by the given client.

        :param client: The object representing the client sending the result.
        :type client: ClientInstance
        :param body: The tuple consisting of the task id and the result.
        :type body: tuple
        """
        id, result = body
        my_print(Verbosity.all, f"Client {client.name} - result for task {id}")
        client.unregister_task(id)
        task = self.tasks[id]
        task.result = result
            
    def process_report_hard_task(self, _client, task_id: int):
        """
        If the task specified by :paramref:`task_id` is minimally hard, then:
        
        1. Add the task to ``self.min_hard``.
        2. Send the :any:`MessageType.APPLY_DOMINO_EFFECT` message to all clients, so they can terminate any task that is as hard or harder than this task.

        :param task_id: The id of the task that timed out.
        :type client: int
        """
        hardness = self.tasks[task_id].hardness
        if self.is_hard(hardness): return # not minimally hard
        self.min_hard.append(hardness)
        
        for c in self.clients:
            c.unregister_domino(self.tasks, hardness)
            self.message_to_instance(
                c, MessageType.APPLY_DOMINO_EFFECT, hardness)

    def process_bye(self, client: ClientInstance, _body):
        """
        Process the :any:`MessageType.BYE` message sent by the given client by terminating the corresponding client instance.

        :param client: The object representing the client sending the message.
        :type client: ClientInstance
        """
        assert(is_client(client))
        my_print(Verbosity.all, 
                f"Got bye from {client.name}; {len(client.my_tasks)} registered tasks remain")
        self.kill_instance(client)

    def process_new_client(self, _instance, body: tuple):
        """
        Construct a client object corresponding to the client that shook hands with the primary server and about which the primary server sent the :any:`MessageType.NEW_CLIENT` message. After constructing the object, invoke its :any:`shake_hands<ClientInstance.shake_hands>` method. The :py:meth:`process_new_client` method is only invoked at the backup server.

        :param body: The information about the new client instance.
        :type body: tuple
        """        
        assert(self.is_backup())
        self.engine.next_instance_name(InstanceRole.CLIENT)
        client = ClientInstance(None, self.tasks_from_failed)
        client.name, client.ip, client.port_primary, client.port_backup, \
            client.active_timestamp = body
        my_print(Verbosity.all, f"New {client.name}")
        client.shake_hands(self.role, self.output_folder)
        self.clients.append(client)

    def process_client_terminated(self, _instance, client_name: str):
        """
        Process client failure reported by the primary server. This method is only invoked at the backup server.

        :param client_name: The name of the terminated client.
        :type client_name: str
        """
        assert(self.is_backup())
        my_print(Verbosity.all, 
                f"Client {client_name} was terminated by primary server")
        self.kill_client(client_name)

    def process_message(self, instance: Instance, type: MessageType, body: Any):
        """
        Process a message from clients and the other server.

        :param instance: The object representing the sending instance.
        :type instance: Instance
        :param type: The type of the message.
        :type type: MessageType
        :param body: The body of the message.
        :type body: Any
        """        
        {
        MessageType.HEALTH_UPDATE: self.process_health_update,
        MessageType.REQUEST_TASKS: self.process_request_tasks,
        MessageType.RESULT: self.process_result,
        MessageType.REPORT_HARD_TASK: self.process_report_hard_task,
        MessageType.LOG: self.process_log,
        MessageType.EXCEPTION: self.process_exception,
        MessageType.BYE: self.process_bye,

        MessageType.NEW_CLIENT: self.process_new_client,
        MessageType.CLIENT_TERMINATED: self.process_client_terminated,
        } [type](instance, body)

    def handle_messages(self):
        """
        Handle messages from clients and the other server.
        """
        for instance in \
            self.clients + [self.primary_server, self.backup_server]:
            
            while self.messages_waiting(instance):
                message_id, type, body = instance.inbound_q.get_nowait()
                
                my_print(type != MessageType.HEALTH_UPDATE and \
                        Verbosity.all_non_health_messages, 
                        f"Got {message_id} of type {type} from {instance.role}")
                if self.is_primary():
                    if is_client(instance):
                        if type != MessageType.HEALTH_UPDATE:
                            my_print(Verbosity.messages, 
                                    f"Handling message {message_id} ({type}) from {instance.name}")
                            self.forward_message(instance, message_id, type, body)
                    self.process_message(instance, type, body)
                    continue

                assert(self.is_backup())
                if is_client(instance): 
                    if type != MessageType.HEALTH_UPDATE:
                        received_id = instance.received_ids[0]
                        if received_id == message_id: 
                            instance.received_ids.pop(0)
                        if message_id > received_id:
                            my_print(Verbosity.all, 
                                    f"{instance.name}   {type}   {body}   message_id={message_id}   received_id={received_id}")
                            assert(False)
                    continue

                assert(is_primary(instance))
                if type != MessageType.MESSAGE_FROM_CLIENT:
                    self.process_message(instance, type, body)
                    continue
                
                assert(type == MessageType.MESSAGE_FROM_CLIENT)
                name, orig_id, message_type, message_body = body
                client = self.get_client(name)
                if not client:
                    my_print(Verbosity.all, 
                            f"The instance object {name} does not exist anymore")
                    return

                my_print(Verbosity.messages, 
                        f"Handling message {orig_id} ({message_type}) from {client.name}")
                client.received_ids.append(orig_id)
                self.process_message(client, message_type, message_body)

#endregion PROCESSING MESSAGES