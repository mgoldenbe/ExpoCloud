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
    def __init__(self, engine):
        self.engine = engine
        self.active_timestamp = None
        self.name, self.ip = None, None
        result = engine.run_instance(InstanceType.CLIENT)
        self.name, self.ip = result
        self.creation_timestamp = time.time()
    
    def __del__(self):
        if self.active_timestamp:
            self.events_file.close()
            self.exceptions_file.close()
        if self.name:
            self.engine.kill_instance(self.name)
    
    def shake_hands(self, port: int, parent_dir: str):
        self.active_timestamp = time.time()
        self.port = port
        self.to_server_q, self.to_client_q = get_client_queues(self.ip, port)
        path = os.path.join(parent_dir, self.ip)
        os.makedirs(path, exist_ok=True)
        self.events_file = open(os.path.join(path, 'events.txt'), "w")
        self.exceptions_file = open(os.path.join(path, 'exceptions.txt'), "w")

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
        
    def process_request_tasks(self, client: Client, n: int):
        tasks = []
        while n > 0 and self.next_task < len(self.tasks):
            task = self.tasks[self.next_task]
            self.next_task += 1
            if self.is_hard(task.hardness): continue
            tasks.append(task)
            n -= 1
        if tasks:
            try:
                client.to_client_q.put((MessageType.GRANT_TASKS, tasks))
            except Exception as e:
                handle_exception(e, "Failed to send tasks")
        if n > 0: client.to_client_q.put((MessageType.NO_FURTHER_TASKS, None))

    def process_log(self, client, descr):
        print(descr, file=client.events_file, flush=True)

    def process_exception(self, client, descr):
        print(descr, file=client.exceptions_file, flush=True)

    def process_result(self, client, body):
        id, result = body
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
            c.to_client_q.put((MessageType.APPLY_DOMINO_EFFECT, hardness))

    def process_bye(self, client, _body):
        print('Got bye from', client.ip, flush=True)
        self.clients = \
            list(filter(lambda c: c.ip != client.ip, self.clients))

    def handle_messages(self):
        for c in self.clients:
            if not c.active_timestamp: continue
            while not c.to_server_q.empty():
                type, body = c.to_server_q.get_nowait()
                {MessageType.REQUEST_TASKS: self.process_request_tasks,
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
        if self.engine.can_create_instance():
            self.clients.append(Client(self.engine))

    def kill_non_active_clients(self):
        overdue = lambda c: time.time() - c.creation_timestamp > \
                            Constants.CLIENT_MAX_NON_ACTIVE_TIME
        self.clients = \
            list(filter(lambda c: c.active_timestamp or not overdue(c), 
                        self.clients))

    def run(self):
        print(f"Got {len(self.tasks)} tasks and ready for clients", flush=True)
        while self.next_task < len(self.tasks) or self.clients:
            self.accept_handshakes()
            self.handle_messages()
            self.create_client()
            self.kill_non_active_clients()
            time.sleep(Constants.SERVER_CYCLE_WAIT)
        self.print_results()