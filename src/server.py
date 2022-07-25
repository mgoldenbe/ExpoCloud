# @ Meir Goldenberg The module is part of the ExpoCloud Framework

import pickle
import subprocess
import time
import os
import sys

from src import util
from src.util import InstanceRole, MessageType, handle_exception, my_ip
from src.constants import Constants
from src.instance import Instance, ClientInstance, BackupServerInstance, PrimaryServerInstance, is_primary, is_backup, is_client

class Server():
    """
    An instance of this class is either a primary or a backup server.
    """
    def __init__(self, tasks, engine, backup, 
                 max_clients = None, max_cpus_per_client = None, 
                 min_group_size = 0):
        """
        Note that this constructor is only ever involked for building the first primary server.
        `backup` - whether a backup server should be used.
        `min_group_size` - minimal size of group defined by the Task's `group_parameter_titles` method.
        """
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
        self.results_file = \
            open(os.path.join(self.output_folder, 'results.txt'), "w")
        
        self.primary_server = None
        self.backup_server = None
        self.to_client_id = 1000 # id of the next outbound message to a client
        
    def __del__(self):
        print("Shutting down", flush=True)
        self.results_file.close()
        if self.is_primary():
            self.handshake_manager.shutdown()

    def run(self):
        if self.is_primary():
            print(f"Got {len(self.tasks)} tasks and ready for clients", 
                  flush=True)

        try:
            while self.tasks_remain() or self.clients:
                self.send_health_update()
                if self.is_primary(): self.accept_handshakes()
                self.handle_messages()
                if self.is_primary(): self.create_instance()
                self.kill_unhealthy_instances()
                time.sleep(Constants.SERVER_CYCLE_WAIT)
        except Exception as e:
            handle_exception(e, "Exception in Server.run")
        
        self.print_results()

#region TASKS

    def tasks_remain(self):
        """
        Return True if tasks to execute remain.
        """
        return self.tasks_from_failed or self.next_task < len(self.tasks)

    def is_hard(self, hardness):
        """
        Check whether `hardness` is hard.
        """
        for h in self.min_hard:
            if hardness >= h: return True
        return False

    def print_results(self):
        """
        Restore the original order of tasks and print results.
        """
        group_counts = {}
        for t in self.tasks:
            group = t.group_parameters()
            if group not in group_counts: group_counts[group] = 0
            group_counts[group] += 1

        self.tasks.sort(key = lambda t: t.orig_id)
        print(util.tuple_to_csv(self.tasks[0].parameter_titles() + \
                                self.tasks[0].result_titles()),
              file = self.results_file)
        for t in self.tasks:
            if not t.result: continue
            if group_counts[t.group_parameters()] >= self.min_group_size:
                print(util.tuple_to_csv(t.parameters() + t.result), 
                      file = self.results_file)

#endregion TASKS

#region ROLES

    def is_primary(self):
        return self.role == InstanceRole.PRIMARY_SERVER

    def is_backup(self):
        return self.role == InstanceRole.BACKUP_SERVER

    def assume_backup_role(self):
        """
        This method is called after unpickling the primary server object, so as to convert it to a backup server one.
        """
        assert(self.is_primary())
        self.role = InstanceRole.BACKUP_SERVER
        self.output_folder = util.output_folder(util.command_arg_name())
        self.results_file = \
            open(os.path.join(self.output_folder, 'results.txt'), "w")
        self.backup_server = None

        self.port = util.get_unused_port()
        self.primary_server = PrimaryServerInstance(self.port)
        util.handshake(self.role, self.port)
        print("Handshake with primary server complete", flush=True)

        if len(self.clients) > 0 and not self.clients[-1].active_timestamp:
            self.clients = self.clients[:-1]
            
        self.update_client_connections()
        for c in self.clients: 
            c.engine = None
            c.received_ids = []
            c.init_files(self.output_folder)

    
    def assume_primary_role(self):
        """
        Assume the role of the primary server. This is called when primary server failure is detected.
        """
        assert(self.is_backup())
        self.role = InstanceRole.PRIMARY_SERVER
        self.primary_server = None
        self.backup_server = None
        self.init_handshake_q()
        for c in self.clients: c.engine = self.engine
        
#endregion ROLES

#region INSTANCES

    def init_handshake_q(self):
        self.handshake_manager = \
            util.make_manager(['handshake_q'], self.port)
        self.handshake_q = self.handshake_manager.handshake_q()

    def handshake_from_client(self, name, port):
        client = self.get_client(name)
        if not client:
            print(f"Unknown client {name} tried to connect",
                    file=sys.stderr, flush=True)
            return
        client.port = port
        client.shake_hands(self.role, self.output_folder)

        # inform the backup server
        print(f"Informing backup server of {client.name}", flush=True)
        self.message_to_instance(
            self.backup_server, MessageType.NEW_CLIENT, 
                (client.name, client.ip, client.port, client.active_timestamp))
    
    def handshake_from_backup(self, name, port):
        if name != self.backup_server.name:
            print(f"Unknown backup server {name} tried to connect",
                  file=sys.stderr, flush=True)
            return
        self.backup_server.port = port
        self.backup_server.shake_hands()
        self.resume_clients()

    def accept_handshakes(self):
        while not self.handshake_q.empty():
            role, name, port = self.handshake_q.get_nowait()
            assert(role == InstanceRole.CLIENT or 
                   role == InstanceRole.BACKUP_SERVER)
            if role == InstanceRole.CLIENT and self.clients_stopped_timestamp:
                self.handshake_q.put((role, name, port))
                continue
            {InstanceRole.CLIENT: self.handshake_from_client,
             InstanceRole.BACKUP_SERVER: self.handshake_from_backup} \
             [role](name, port)

    def n_active_clients(self):
        return len(list(filter(lambda c: c.active_timestamp, self.clients)))

    def get_client(self, name):
        try:
            return next(filter(lambda c: c.name == name, self.clients))
        except:
            return None

    def kill_client(self, name):
        self.clients = \
            list(filter(lambda c: c.name != name, self.clients))

    def kill_instance(self, instance):
        if is_client(instance):
            self.kill_client(instance.name)
            return
        if is_backup(instance):
            self.backup_server = None
            self.stop_clients()
            return
        assert(False)
    
    def create_backup_server_instance(self):
        def pickle_server():
            # exclude things that should not be pickled/unpickled
            temp_handshake_manager, temp_handhsake_q, temp_results_file, temp_backup_server = \
                self.handshake_manager, self.handshake_q, self.results_file, self.backup_server
            self.handshake_manager, self.handshake_q, self.results_file, self.backup_server = \
                None, None, None, None
            client_files = []
            for c in self.clients:
                if not c.active_timestamp: continue
                client_files.append((c.events_file, c.exceptions_file))
                c.events_file, c.exceptions_file = None, None
            with open(util.pickled_file_name(), 'wb') as f:
                pickle.dump(self, f)
            self.handshake_manager, self.handshake_q, self.results_file, self.backup_server = \
                temp_handshake_manager, temp_handhsake_q, temp_results_file, temp_backup_server
            for c in self.clients:
                if not c.active_timestamp: continue
                c.events_file, c.exceptions_file  = client_files.pop(0)
        
        def copy_output_folder():
            backup_output_folder = \
                    util.output_folder(self.backup_server.name)
            if not self.engine.is_local():
                util.remote_replace(
                    self.output_folder,
                    self.backup_server.ip, 
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
        # If no more tasks, don't create another client
        if not self.tasks_remain(): return

        # Make a client if all existing clients have ip
        if len(self.clients) == 0 or self.clients[-1].ip:
            print("creating instance object", flush=True)
            self.clients.append(
                ClientInstance(self.engine, self.tasks_from_failed))
        client = self.clients[-1]
        if not client.ip: client.create()
        if client.ip: client.run(self.port, self.max_cpus_per_client)

    def create_instance(self):
        if self.backup and \
            (not self.backup_server or not self.backup_server.active_timestamp):
            self.create_backup_server_instance()
        else:
            if (not self.max_clients) or \
               self.n_active_clients() < self.max_clients:
                self.create_client_instance()

    def kill_unhealthy_instances(self):
        if self.is_backup():
            if not self.primary_server.is_healthy():
                print("Primary server failed", flush=True)
                self.assume_primary_role()
                for c in self.clients:
                    temp = c.outbound_q
                    try:
                        c.outbound_q = util.get_guest_qs(
                            c.ip, c.port, ['from_primary_q'])
                    except:
                        print("Temporary connection to from_primary_q failed",
                              flush=True)
                    self.message_to_instance(c, MessageType.SWAP_QUEUES, None)
                    c.outbound_q = temp
                    c.engine = self.engine
                    
            return
        
        assert(self.is_primary())
        tasks_remain = self.tasks_remain()
        for c in self.clients:
            if c.is_healthy(tasks_remain): continue
            self.kill_client(c.name)
            self.message_to_instance(
                self.backup_server, MessageType.CLIENT_FAILURE, c.name)
        
        if self.backup_server and \
           not self.backup_server.is_healthy(tasks_remain):
            self.backup_server = None

    def stop_clients(self):
        for c in self.clients:
            self.message_to_instance(c, MessageType.STOP, None)
        self.clients_stopped_timestamp = time.time()
    
    def resume_clients(self):
        for c in self.clients:
            self.message_to_instance(c, MessageType.RESUME, None)
        self.clients_stopped_timestamp = None

    def update_client_connections(self):
        for c in self.clients:
            if not c.active_timestamp: continue
            c.connect(self.role)

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
    def message_to_instance(self, instance, type, body):
        if not instance or not instance.active_timestamp:
            return

        try:
            message = (type, body)
            if is_client(instance) and \
               type not in [MessageType.STOP, MessageType.RESUME]:
                message = (self.to_client_id,) + message
                print(f"Sending message {self.to_client_id} ({type}) to {instance.name}", flush=True)
                self.to_client_id += 1
            else:
                message = (None,) + message
            instance.outbound_q.put(message)
        except:
            print(f"Instance {instance.name} failed", flush=True)
            self.kill_instance(instance)

    def messages_waiting(self, instance):
        """
        Checks whether a message from `instance` can be read.
        If intance is invalid or is not active, return False.
        If there is no message in the inbound queue from the `instance`, return False. Otherwise return True, unless it is the backup server, `instance` is a client and there is no stored message id for this client.
        """
        try:
            if not instance or not instance.active_timestamp: return False
            if instance.inbound_q.empty(): return False
            
            if self.is_primary(): return True

            assert(self.is_backup())
            if not is_client(instance): return True
            if not instance.received_ids: return False
            id = instance.inbound_q.queue[0][0]
            received_id = instance.received_ids.pop(0)
            assert(id == received_id)
            return True
        except:
            print(f"Instance {instance.name} failed", flush=True)
            self.kill_instance(instance)

    def forward_message(self, instance, message_id, type, body):
        """
        Forward messages from clients except health updates to backup server.
        """
        if instance.role != InstanceRole.CLIENT: return
        if type == MessageType.HEALTH_UPDATE: return
        self.message_to_instance(
            self.backup_server, MessageType.MESSAGE_FROM_CLIENT, 
            (instance.name, message_id, type, body))

#endregion MESSAGES

#region PROCESSING MESSAGES

    def process_health_update(self, instance, _body):
        instance.active_timestamp = time.time()

    def process_request_tasks(self, client: ClientInstance, n: int):
        tasks = []
        while n > 0 and self.tasks_remain():
            task = self.tasks[self.next_task]
            if self.tasks_from_failed:
                task = self.tasks[self.tasks_from_failed.pop(0)]
            else:
                self.next_task += 1
            if self.is_hard(task.hardness):
                print(f"Skipping hard task {task.id}", flush=True)
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

    def process_log(self, client, descr):
        print(descr, file=client.events_file, flush=True)

    def process_exception(self, client, descr):
        print(descr, file=client.exceptions_file, flush=True)

    def process_result(self, client, body):
        id, result = body
        print(f"Client {client.name} - result for task {id}", 
              flush=True)
        client.unregister_task(id)
        task = self.tasks[id]
        task.result = result
            
    def process_report_hard_task(self, _client, task_id):
        """
        Handle the overdue task: 
        1. After checking again that the task is minimally hard, add the task to self.min_hard.
        2. Send this task to all clients for application of domino effect. Note: this is suboptimal as far as network capacity is concerned, but simplifies the server, which does not need to maintain which tasks are currently worked on by each client. 
        """
        hardness = self.tasks[task_id].hardness
        if self.is_hard(hardness): return # not minimally hard
        self.min_hard.append(hardness)
        
        for c in self.clients:
            c.unregister_domino(self.tasks, hardness)
            self.message_to_instance(
                c, MessageType.APPLY_DOMINO_EFFECT, hardness)

    def process_bye(self, client, _body):
        assert(is_client(client))
        print(f"Got bye from {client.name}; {len(client.my_tasks)} registered tasks remain", flush=True)
        self.kill_instance(client)

    def process_new_client(self, _instance, body):
        assert(self.is_backup())
        client = ClientInstance(None, self.tasks_from_failed)
        client.name, client.ip, client.port, client.active_timestamp = body
        print(f"New {client.name}", flush=True)
        client.shake_hands(self.role, self.output_folder)
        self.clients.append(client)

    def process_client_failure(self, _instance, name):
        """
        Process client failure reported by the primary server.
        """
        assert(self.is_backup())
        self.kill_client(name)

    def process_message(self, instance, type, body):
        {
        MessageType.HEALTH_UPDATE: self.process_health_update,
        MessageType.REQUEST_TASKS: self.process_request_tasks,
        MessageType.RESULT: self.process_result,
        MessageType.REPORT_HARD_TASK: self.process_report_hard_task,
        MessageType.LOG: self.process_log,
        MessageType.EXCEPTION: self.process_exception,
        MessageType.BYE: self.process_bye,

        MessageType.NEW_CLIENT: self.process_new_client,
        MessageType.CLIENT_FAILURE: self.process_client_failure,
        } [type](instance, body)

    def handle_messages(self):
        """
        Handle messages from instances.
        """
        for instance in \
            self.clients + [self.primary_server, self.backup_server]:
            
            while self.messages_waiting(instance):
                message_id, type, body = instance.inbound_q.get_nowait()
                #if type != MessageType.HEALTH_UPDATE: print(f"Got {message_id} of type {type} from {instance.role}", flush=True)
                if self.is_primary():
                    if is_client(instance):
                        #print(f"Handling message {message_id} ({type}) from {instance.name}", flush=True)
                        self.forward_message(instance, message_id, type, body)
                    self.process_message(instance, type, body)
                    continue

                assert(self.is_backup())
                if is_client(instance):
                    continue

                assert(is_primary(instance))
                if type != MessageType.MESSAGE_FROM_CLIENT:
                    self.process_message(instance, type, body)
                    continue
                
                assert(type == MessageType.MESSAGE_FROM_CLIENT)
                name, orig_id, message_type, message_body = body
                client = self.get_client(name)
                assert(client)
                
                #print(f"Handling message {orig_id} ({message_type}) from {client.name}", flush=True)
                client.received_ids.append(orig_id)
                self.process_message(client, message_type, message_body)

#endregion PROCESSING MESSAGES