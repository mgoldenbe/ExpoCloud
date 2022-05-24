# @ Meir Goldenberg The module is part of the ExpoCloud Framework

from multiprocessing import Process, Value, Queue, cpu_count
import time
import sys
import socket
from multiprocessing.managers import SyncManager

from pkg_resources import working_set

from src import util
from src.util import MessageType
from src.constants import Constants

# Responsible for a single task
class Worker(Process):
    def __init__(self, id, task, results_q):
        Process.__init__(self)
        self.id = id
        self.task = task
        self._results_q = results_q
        self.timestamp = Value('d', 0)
        self.killed = False # takes time after kill() before is_alive() == False

    def my_kill(self):
        self.killed = True
        self.kill()

    def run(self):
        self.timestamp.value = time.time()
        util.print_event("starting", self)
        self.task.result = self.task.run() # task should handle exceptions
        util.print_event("done", self)
        self._results_q.put(self.task)

def handshake():
    try:
        server_ip = sys.argv[1]
        server_port = Constants.SERVER_PORT
        my_port = Constants.CLIENT_PORT
        auth = b'myauth'
    except Exception as e:
        util.handle_exception(e, f"Wrong command-line arguments {sys.argv}")

    try:
        class MyManager(SyncManager):
            pass
        MyManager.register('handshake_q')
        manager = MyManager(address=(server_ip, server_port), authkey=auth)
        manager.connect()
        manager.handshake_q().put((util.my_ip(), my_port))
    except Exception as e:
        util.handle_exception(e, 'Handshake with the server failed')

def make_manager():
    to_server_q = Queue()
    to_client_q = Queue()
    class MyManager(SyncManager):
        pass
    MyManager.register('to_server_q', callable=lambda: to_server_q)
    MyManager.register('to_client_q', callable=lambda: to_client_q)

    auth = b'myauth'
    try:
        manager = MyManager(address=('', Constants.CLIENT_PORT), authkey=auth)
        manager.start()
    except Exception as e:
        util.handle_exception(e, 'Could not start manager')

    return manager

class Client:
    """
    The main client class.
    """

    def __init__(self):
        """
        Perform handshake with the server and create the queues:
        self.bye_q - to inform the server when this client is done.
        self.request_tasks_q - to request tasks from the server.
        self.grant_tasks_q - to receive tasks from the server.
        """
        self.workers = []
        self.next_worker_id = 0
        self.name = socket.gethostname()
        self.titles_flag = False # whether have already output column titles
        self.tasks = [] # tasks not yet assigned to workers
        self.done_tasks = []
        self.n_requested = 0 # number of tasks requested, but not granted yet
        self.no_further_tasks = False # True - no more tasks at the server
        self.results_q = Queue()
        self.capacity = cpu_count()

        self.manager = make_manager()
        self.to_server_q, self.to_client_q = \
            self.manager.to_server_q(), self.manager.to_client_q()
        handshake()

    def process_grant_tasks(self, tasks):
        self.n_requested -= len(tasks)
        self.tasks += tasks

        if not self.titles_flag:
            # column titles
            self.titles_flag = True
            print("timestamp,descr,worker_id,task_id," + \
                  util.tuple_to_csv(tasks[0].parameter_titles()),
                  file=sys.stderr, flush=True)
        
        for t in tasks:
            util.print_event("received", task=t)
    
    def apply_domino_effect(self, hard):
        """
        1. Kills the workers that execute tasks harder than `hard`.
        2. Removes tasks harder than `hard` from self.tasks.
        """
        for w in self.workers:
            if w.killed: continue
            if w.task.hardness >= hard:
                util.print_event("domino", w)
                w.my_kill()
                
        filter(lambda t: t.hardness < hard, self.tasks)

    def process_no_further_tasks(self, _body):
        self.no_further_tasks = True

    def process_messages(self):
        while not self.to_client_q.empty():
            type, body = self.to_client_q.get_nowait()
            {MessageType.GRANT_TASKS: self.process_grant_tasks,
             MessageType.APPLY_DOMINO_EFFECT: self.apply_domino_effect,
             MessageType.NO_FURTHER_TASKS: self.process_no_further_tasks} \
             [type](body)

    def request_tasks(self, n: int):
        """
        Request n tasks from the server.
        """
        if n == 0: return
        self.n_requested += n
        self.to_server_q.put((MessageType.REQUEST_TASKS, n))
        
    def collect_results(self):
        while not self.results_q.empty():
            self.done_tasks.append(self.results_q.get_nowait())

    def collect_done(self):
        self.collect_results()
        self.workers = list(\
            filter(lambda worker: worker.is_alive(), 
                   self.workers))

    def kill_overdue(self):
        for worker in self.workers:
            if worker.killed: continue
            before = worker.timestamp.value
            if before and time.time() - before > worker.task.timeout:
                util.print_event("timeout", worker)
                worker.my_kill()
                self.to_server_q.put((MessageType.REPORT_HARD_TASK, 
                                      worker.task.id))                

    def process_workers(self):
        self.collect_done()
        self.kill_overdue()

    def occupy_workers(self):
        while len(self.workers) < self.capacity and self.tasks:
            task = self.tasks.pop(0)
            worker = Worker(self.next_worker_id, task, self.results_q)
            self.next_worker_id += 1
            self.workers.append(worker)
            worker.start()

    def run(self):
        while self.tasks or not self.no_further_tasks:
            self.process_workers()
            if not self.no_further_tasks:
                n_tasks_in_pipeline = \
                    len(self.workers) + len(self.tasks) + self.n_requested
                self.request_tasks(self.capacity - n_tasks_in_pipeline)
            self.process_messages()
            self.occupy_workers()
            time.sleep(Constants.CLIENT_CYCLE_WAIT)

        while self.workers:
            self.process_workers()
            time.sleep(Constants.CLIENT_CYCLE_WAIT)

        # restore order and print results
        self.done_tasks.sort(key = lambda t: t.orig_id)
        if self.done_tasks:
            print(util.tuple_to_csv(self.done_tasks[0].parameter_titles() + \
                                    self.done_tasks[0].result_titles()))
            for t in self.done_tasks:
                print(util.tuple_to_csv(t.parameters() + t.result))
                
        self.to_server_q.put((MessageType.BYE, None))
        time.sleep(Constants.CLIENT_WAIT_AFTER_SENDING_BYE)
        self.manager.shutdown()

        # TODO: form dictionary parameters => number of done tasks
        # Filter done tasks to include only those with enough done tasks with the same parameters.
        # See if tasks_dict is used.