# @ Meir Goldenberg The module is part of the ExpoCloud Framework

from multiprocessing import Queue
from multiprocessing.managers import SyncManager

import time
import os
import sys
from src.abstract_engine import InstanceType
from src import util
from src.util import MessageType, handle_exception
from src.constants import Constants

def get_client_queues(ip, port):
    class MyManager(SyncManager):
        pass
    
    MyManager.register('to_server_q')
    MyManager.register('to_client_q')
    auth = b'myauth'
    manager = MyManager(address=(ip, port), authkey=auth)
    manager.connect()
    return manager.to_server_q(), manager.to_client_q()

def get_handshake_manager():
    handshake_q = Queue()
    class MyManager(SyncManager):
        pass
    MyManager.register('handshake_q', callable=lambda: handshake_q)
    auth = b'myauth'
    try:
        manager = MyManager(address=('', Constants.SERVER_PORT), authkey=auth)
        manager.start()
    except Exception as e:
        util.handle_exception(e, 'Could not start manager')

    return manager

class Client():
    def __init__(self, engine, tasks_from_failed):
        """
        Objects of this class represent clients. 
        `engine` - an instance of either AbstractEngine's subclass or LocalEngine.
        `tasks_from_failed` - the list to which the tasks assigned to this client are to be appended should this client fail.
        """
        self.engine = engine
        self.tasks_from_failed = tasks_from_failed
        self.my_tasks = [] # ids of tasks held by the client
        self.active_timestamp = None # last health update, None until handshake
        self.name, self.ip = None, None
        result = engine.run_instance(InstanceType.CLIENT)
        if result:
            self.name, self.ip = result
            print(f"New client at {self.ip}", flush = True)
        else:
            print(f"Creation of new client failed", file=sys.stderr, flush=True)
        self.creation_timestamp = time.time()
    
    def __del__(self):
        print(f"The client {self.name} at {self.ip} is dying", 
              file=sys.stderr, flush=True)
        if self.active_timestamp:
            self.events_file.close()
            self.exceptions_file.close()
        if self.name:
            self.engine.kill_instance(self.name)
        self.tasks_from_failed += self.my_tasks
    
    def is_healthy(self, tasks_remain: bool):
        """
        Returns true if the client is healthy.
        A client is healthy if either:
        - It is not active, but has an ip address, and there are still tasks remaining as indicated by the `tasks_remain` argument CLIENT_MAX_NON_ACTIVE_TIME has not passed since its creation.
        - It is active and HEALTH_UPDATE_LIMIT has not passed since last health 
          update.
        """
        if (not self.active_timestamp) and self.ip and tasks_remain:
            if time.time() - self.creation_timestamp <= \
               Constants.CLIENT_MAX_NON_ACTIVE_TIME: return True

        if self.active_timestamp:
            if time.time() - self.active_timestamp <= \
               Constants.HEALTH_UPDATE_LIMIT: return True

        if self.active_timestamp or tasks_remain:
            print(f"The client {self.name} at {self.ip} is unhealthy", 
                  file=sys.stderr, flush=True)
            print(f"Created {self.creation_timestamp}",
                  f"last healthy {self.active_timestamp}", 
                  file=sys.stderr, flush=True)
        else:
            print(f"Inactive client {self.name} at {self.ip} remains", 
                  file=sys.stderr, flush=True)
        return False

    def shake_hands(self, port: int, parent_dir: str):
        self.active_timestamp = time.time()
        self.port = port
        self.to_server_q, self.to_client_q = get_client_queues(self.ip, port)
        path = os.path.join(parent_dir, self.ip)
        os.makedirs(path, exist_ok=True)
        self.events_file = open(os.path.join(path, 'events.txt'), "w")
        self.exceptions_file = open(os.path.join(path, 'exceptions.txt'), "w")

    def register_tasks(self, tasks):
        self.my_tasks += [t.id for t in tasks]
    
    def unregister_task(self, t_id):
        self.my_tasks = list(filter(lambda i: i != t_id, self.my_tasks))

    def unregister_domino(self, tasks, hardness):
        hard = [t_id for t_id in self.my_tasks 
                if tasks[t_id].hardness >= hardness]
        self.my_tasks = list(filter(lambda i: i not in hard, self.my_tasks))

class Cluster():
    """

    """
    def __init__(self, tasks, engine, 
                 min_group_size = 0, output_folder = 'output'):
        """
        `min_group_size` - minimal size of group defined by the Task's `group_parameter_titles` method.
        """
        self.engine = engine
        self.clients = []
        self.handshake_manager = get_handshake_manager()
        self.handshake_q = self.handshake_manager.handshake_q()

        # Store original order for results output, then sort by difficulty
        for i, t in enumerate(tasks): 
            t.orig_id = i
        self.tasks = sorted(tasks, key = lambda t: t.hardness)
        self.next_task = 0 # next task to be given to clients
        self.tasks_from_failed = [] # tasks from failed clients to reassign
        self.min_hard = [] # hardness for each minimally hard task
        self.group_counts = {} # number of done tasks for each group
        self.min_group_size = min_group_size

        for i, t in enumerate(self.tasks):
            t.id = i
            t.result = None
        
        self.output_folder = output_folder
        os.makedirs(self.output_folder, exist_ok=True)
        self.results_file = \
            open(os.path.join(self.output_folder, 'results.txt'), "w")
        
    def __del__(self):
        self.results_file.close()
        self.handshake_manager.shutdown()

    def is_hard(self, hardness):
        """
        Check whether `hardness` is hard.
        """
        for h in self.min_hard:
            if hardness >= h: return True
        return False

    def process_health_update(self, client, _body):
        client.active_timestamp = time.time()

    def accept_handshakes(self):
        while not self.handshake_q.empty():
            client_ip, client_port = self.handshake_q.get_nowait()
            try:
                client = next(filter(lambda c: c.ip == client_ip, self.clients))
            except Exception as e:
                util.handle_exception(
                    e, f"Unknown client tried to connect from {client_ip}", 
                    exit_flag=False)
                continue
            client.shake_hands(client_port, self.output_folder)

    def message_to_client(self, client, type, body):
        try:
            client.to_client_q.put((type, body))
        except Exception as e:
            handle_exception(
                e, f"Message {type} failed to client at {client.ip}", False)
            self.kill_client(client)
    
    def client_messages(self, client):
        try:
            return not client.to_server_q.empty()
        except Exception as e:
            handle_exception(
                e, f"Message check from client at {client.ip} failed", False)
            self.kill_client(client)

    def tasks_remain(self):
        """
        Return True if tasks to execute remain.
        """
        return self.tasks_from_failed or self.next_task < len(self.tasks)

    def process_request_tasks(self, client: Client, n: int):
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
                self.message_to_client(client, MessageType.GRANT_TASKS, tasks)
            except Exception as e:
                handle_exception(e, "Failed to send tasks")
        if n > 0:
            self.message_to_client(client, MessageType.NO_FURTHER_TASKS, None)

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
        group = task.group_parameters()
        if group not in self.group_counts: self.group_counts[group] = 0
        self.group_counts[group] += 1
            
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
            self.message_to_client(c, MessageType.APPLY_DOMINO_EFFECT, hardness)

    def kill_client(self, client):
        self.clients = \
            list(filter(lambda c: c.ip != client.ip, self.clients))

    def process_bye(self, client, _body):
        print(f"Got bye from {client.ip}; {len(client.my_tasks)} registered tasks remain", flush=True)
        self.kill_client(client)

    def handle_messages(self):
        for c in self.clients:
            if not c.active_timestamp: continue
            while self.client_messages(c):
                type, body = c.to_server_q.get_nowait()
                {MessageType.HEALTH_UPDATE: self.process_health_update,
                 MessageType.REQUEST_TASKS: self.process_request_tasks,
                 MessageType.LOG: self.process_log,
                 MessageType.EXCEPTION: self.process_exception,
                 MessageType.RESULT: self.process_result,
                 MessageType.REPORT_HARD_TASK: self.process_report_hard_task,
                 MessageType.BYE: self.process_bye}[type](c, body)

    def print_results(self):
        """
        Restore the original order of tasks and print results.
        """
        self.tasks.sort(key = lambda t: t.orig_id)
        print(util.tuple_to_csv(self.tasks[0].parameter_titles() + \
                                self.tasks[0].result_titles()),
              file = self.results_file)
        for t in self.tasks:
            if not t.result: continue
            if self.group_counts[t.group_parameters()] >= self.min_group_size:
                print(util.tuple_to_csv(t.parameters() + t.result), 
                      file = self.results_file)

    def create_client(self):
        if self.engine.creation_attempt_allowed():
            self.clients.append(Client(self.engine, self.tasks_from_failed))

    def kill_unhealthy_clients(self):
        tasks_remain = self.tasks_remain()
        self.clients = \
            list(filter(lambda c: c.is_healthy(tasks_remain), self.clients))

    def run(self):
        print(f"Got {len(self.tasks)} tasks and ready for clients", flush=True)
        while self.tasks_remain() or self.clients:
            self.accept_handshakes()
            self.handle_messages()
            self.create_client()
            self.kill_unhealthy_clients()
            time.sleep(Constants.SERVER_CYCLE_WAIT)
        self.print_results()