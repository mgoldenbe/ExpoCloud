# @ Meir Goldenberg The module is part of the ExpoCloud Framework

from multiprocessing import Queue
from multiprocessing.managers import SyncManager

import time
import sys
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
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port
        self.to_server_q, self.to_client_q = get_client_queues(ip, port)
        
class Cluster():
    """

    """
    def __init__(self, tasks):
        self.clients = []
        print("in Cluster.__init__", file=sys.stderr, flush=True)
        self.handshake_manager = get_handshake_manager()
        self.handshake_q = self.handshake_manager.handshake_q()

        # Store original order for results output, then sort by difficulty
        for i, t in enumerate(tasks): 
            t.orig_id = i
        self.tasks = sorted(tasks, key = lambda t: t.hardness)
        self.next_task = 0 # next task to be given to clients
        self.min_hard = [] # hardness for each minimally hard task

        for i, t in enumerate(self.tasks):
            t.id = i
            t.result = None
        
        # column titles
        print("timestamp,descr,worker_id,task_id," + \
              util.tuple_to_csv(tasks[0].parameter_titles()),
              file=sys.stderr, flush=True)
        # dump tasks
        for t in self.tasks:
            util.print_event("submitted", task=t)

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
            if client_ip in [c.ip for c in self.clients]: continue
            try:
                self.clients.append(Client(client_ip, client_port))
            except:
                pass
        
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
        self.clients = \
            list(filter(lambda c: c.ip != client.ip, self.clients))

    def handle_messages(self):
        for c in self.clients:
            while not c.to_server_q.empty():
                type, body = c.to_server_q.get_nowait()
                {MessageType.REQUEST_TASKS: self.process_request_tasks,
                 MessageType.REPORT_HARD_TASK: self.process_report_hard_task,
                 MessageType.BYE: self.process_bye}[type](c, body)

    def run(self):
        print("Ready for clients", flush=True)
        while self.next_task < len(self.tasks) or self.clients:
            self.accept_handshakes()
            self.handle_messages()
            time.sleep(Constants.SERVER_CYCLE_WAIT)
        self.handshake_manager.shutdown()