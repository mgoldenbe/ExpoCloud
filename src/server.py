# @ Meir Goldenberg The module is part of the ExpoCloud Framework

import pickle
from pydoc import cli
import time
import os
import sys

from src import util
from src.util import InstanceRole, MessageType, handle_exception
from src.constants import Constants
from src.instance import Instance, ClientInstance, BackupServerInstance, PrimaryServerInstance, is_primary, is_backup, is_client

class Server():
    """
    An instance of this class is either a primary or a backup server.
    """
    def __init__(self, tasks, engine, backup, 
                 min_group_size = 0):
        """
        Note that this constructor is only ever involked for building the first primary server.
        `backup` - whether a backup server should be used.
        `min_group_size` - minimal size of group defined by the Task's `group_parameter_titles` method.
        """
        self.role = InstanceRole.PRIMARY_SERVER
        self.engine = engine
        self.backup = backup
        self.clients = []
        self.clients_stopped_timestamp = None

        self.handshake_manager = \
            util.make_manager(['handshake_q'], Constants.SERVER_PORT)
        self.handshake_q = self.handshake_manager.handshake_q()

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
        
        self.output_folder = Constants.OUTPUT_FOLDER
        os.makedirs(self.output_folder, exist_ok=True)
        self.results_file = \
            open(os.path.join(self.output_folder, 'results.txt'), "w")
        
        self.primary_server = None
        self.backup_server = None
        self.to_client_id = 1000 # id of the next outbound message to a client
        
    def __del__(self):
        self.results_file.close()
        self.handshake_manager.shutdown()

    def run(self):
        if self.is_primary():
            print(f"Got {len(self.tasks)} tasks and ready for clients", 
                  flush=True)
        while self.tasks_remain() or self.clients:
            self.send_health_update()
            if self.is_primary(): self.accept_handshakes()
            self.handle_messages()
            self.create_instance()
            self.kill_unhealthy_instances()
            if self.is_backup(): 
                self.check_primary_server()
            time.sleep(Constants.SERVER_CYCLE_WAIT)
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
        self.backup_server = None

        self.primary_server = PrimaryServerInstance()
        util.handshake(self.role)

        self.update_client_connections()
        for c in self.clients: 
            c.engine = None
            c.received_ids = []
    
    def assume_primary_role(self):
        """
        Assume the role of the primary server. This is called when primary server failure is detected.
        """
        assert(self.is_backup())
        self.role = InstanceRole.PRIMARY_SERVER
        self.primary_server = None
        self.backup_server = None
        for c in self.clients: c.engine = self.engine
        
#endregion ROLES

#region INSTANCES

    def handshake_from_client(self, ip):
        client = self.get_client(ip)
        if not client:
            print(f"Unknown client tried to connect from {ip}",
                    file=sys.stderr, flush=True)
            return
        client.shake_hands(self.role, self.output_folder)

        # inform the backup server
        if self.backup_server:
            self.message_to_instance(
                self.backup_server, MessageType.NEW_CLIENT, client.ip)
    
    def handshake_from_backup(self, ip):
        if ip != self.backup_server.ip:
            print(f"Unknown backup server tried to connect from {ip}",
                  file=sys.stderr, flush=True)
            return
        self.backup_server.shake_hands()
        self.resume_clients()

    def accept_handshakes(self):
        while not self.handshake_q.empty():
            role, ip = self.handshake_q.get_nowait()
            assert(role == InstanceRole.CLIENT or 
                   role == InstanceRole.BACKUP_SERVER)
            {InstanceRole.CLIENT: self.handshake_from_client,
             InstanceRole.BACKUP_SERVER: self.handshake_from_backup} \
             [role](ip)

    def get_client(self, ip):
        try:
            return next(filter(lambda c: c.ip == ip, self.clients))
        except:
            return None

    def kill_client(self, ip):
        self.clients = \
            list(filter(lambda c: c.ip != ip, self.clients))

    def kill_instance(self, instance):
        if is_client(instance):
            self.kill_client(instance.ip)
            return
        if is_backup(instance):
            self.backup_server = None
            return
        assert(False)

    def create_instance(self):
        # Creating backup server is first priority
        if self.backup and self.is_primary():
            if not self.backup_server:
                self.backup_server = BackupServerInstance(self.engine)

            assert(self.backup_server)
            if not self.backup_server.ip:
                self.backup_server.create()
                if self.backup_server.ip:
                    # serialize the primary server, i.e. self
                    temp = self.handshake_manager # to restore
                    self.handshake_manager = None # not pickable
                    with open(Constants.PICKLED_SERVER_FILE, 'wb') as f:
                        pickle.dump(self, f)
                    self.handshake_manager = temp # restoring
                    # copy output folder
                    self.engine.remote_replace(
                        self.backup_server.ip, 
                        os.path.join(self.engine.root_folder, 
                                     self.output_folder))
                return
            
            assert(self.backup_server.ip)
            if not self.clients_stopped_timestamp:
                self.stop_clients()

            assert(self.clients_stopped_timestamp)
            if time.time() - self.clients_stopped_timestamp >= \
                Constants.CLIENTS_STOP_TIME:
                self.backup.run()
        
        # If no more tasks, don't create another client
        if not self.tasks_remain: return

        # Make a client if all existing clients have ip
        if len(self.clients) == 0 or self.clients[-1].ip:
            print("creating instance object", flush=True)
            self.clients.append(
                ClientInstance(self.engine, self.tasks_from_failed))
        client = self.clients[-1]
        if not client.ip: client.create()
        if client.ip: client.run()

    def kill_unhealthy_instances(self):
        if self.is_backup():
            if not self.primary_server.is_healthy():
                self.assume_primary_role()
                for c in self.clients:
                    temp = c.outbound_q
                    c.outbound_q = util.get_guest_qs(
                        c.ip, Constants.CLIENT_PORT, ['from_primary_q'])
                    self.message_to_instance(c, MessageType.SWAP_QUEUES, None)
                    c.outbound_q = temp
                    
            return
        
        assert(self.is_primary())
        tasks_remain = self.tasks_remain()
        for c in self.clients:
            if c.is_healthy(tasks_remain): continue
            self.kill_client(c.ip)
            if self.backup_server:
                self.message_to_instance(
                    self.backup_server, MessageType.CLIENT_FAILURE, c.ip)
        
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
        if not other_server: return
        self.message_to_instance(
            other_server, MessageType.HEALTH_UPDATE, None)

#endregion INSTANCES

#region MESSAGES

    def message_to_instance(self, instance, type, body):
        print(f"{type} to {instance.role}", flush=True)
        try:
            message = (type, body)
            if is_client(instance):
                message = (self.to_client_id,) + message
                self.to_client_id += 1
            instance.outbound_q.put(message)
        except Exception as e:
            handle_exception(
                e, f"Message {type} to instance at {instance.ip} failed", False)
            self.kill_instance(instance)

    def messages_waiting(self, instance):
        """
        Checks whether a message from `instance` can be read.
        If there is no message in the inbound queue from the `instance`, return False. Otherwise return True, unless it is the backup server, `instance` is a client and there is no stored message id for this client.
        """
        try:
            if instance.inbound_q.empty(): return False

            if self.is_primary(): return True

            assert(self.is_backup())
            if is_client(instance) and not instance.received_ids: 
                return False
            return True
        except Exception as e:
            handle_exception(
                e, f"Message check from {instance.role} at {instance.ip} failed", False)
            self.kill_instance(instance)

    def forward_message(self, instance, message_id, type, body):
        """
        Forward messages from clients except health updates to backup server.
        """
        if not self.backup_server: return
        if instance.role != InstanceRole.CLIENT: return
        if type == MessageType.HEALTH_UPDATE: return
        self.message_to_instance(
            self.backup_server, MessageType.MESSAGE_FROM_CLIENT, 
            (instance, message_id, type, body))

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
        print(f"Client at {client.ip} - result for task {id}", 
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
        print(f"Got bye from {client.ip}; {len(client.my_tasks)} registered tasks remain", flush=True)
        self.kill_instance(client)

    def process_new_client(self, _instance, ip):
        assert(self.is_backup())
        client = ClientInstance(None, self.tasks_from_failed)
        client.ip = ip
        client.shake_hands(self.role, self.output_folder)
        self.clients.append(client)

    def process_client_failure(self, _instance, ip):
        """
        Process client failure reported by the primary server.
        """
        assert(self.is_backup())
        self.kill_client(ip)

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
            if not instance or not instance.active_timestamp: continue
            while self.messages_waiting(instance):
                message_id, type, body = instance.inbound_q.get_nowait()
                print(f"Got {message_id} of type {type} from client. Body: {body}", flush=True)
                if self.is_primary():
                    if is_client(instance):
                        self.forward_message(instance, message_id, type, body)
                    self.process_message(instance, type, body)
                    continue

                assert(self.is_backup())
                if is_client(instance): continue

                assert(is_primary(instance))
                if type != MessageType.MESSAGE_FROM_CLIENT:
                    self.process_message(instance, type, body)
                    continue
                
                assert(type == MessageType.MESSAGE_FROM_CLIENT)
                received_id = instance.received_ids.pop()
                assert(message_id == received_id)
                ip, message_type, message_body = body
                client = self.get_client(ip)
                self.process_message(client, message_type, message_body)

#endregion PROCESSING MESSAGES