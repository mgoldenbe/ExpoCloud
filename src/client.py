# @ Meir Goldenberg The module is part of the ExpoCloud Framework

from multiprocessing import Process, Value, Queue, cpu_count
import time
import sys
import socket
from multiprocessing.managers import SyncManager

from pkg_resources import working_set

from src import util
from src.util import InstanceRole, MessageType
from src.constants import Constants

# Responsible for a single task
class Worker(Process):
    def __init__(self, id, task, queue):
        Process.__init__(self)
        self.id = id
        self.task = task
        self.queue = queue
        self.timestamp = Value('d', 0)
        self.killed = False # takes time after kill() before is_alive() == False

    def my_kill(self):
        self.killed = True
        self.kill()

    def run(self):
        print(f"Worker {self.id} with {self.task.id}", flush=True)
        self.timestamp.value = time.time()
        self.queue.put((MessageType.WORKER_STARTED, None))
        result = self.task.run()
        self.queue.put((MessageType.WORKER_DONE, result))

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
        self.begin_timestamp = time.time()
        self.workers = []
        self.next_worker_id = 0
        self.name = socket.gethostname()
        self.titles_flag = False # whether have already output column titles
        self.tasks = [] # tasks not yet assigned to workers
        self.done_tasks = []
        self.n_requested = 0 # number of tasks requested, but not granted yet
        self.no_further_tasks = False # True - no more tasks at the server
        self.capacity = cpu_count()
        if sys.argv[4] != "None":
            self.capacity = min(self.capacity, int(sys.argv[4]))

        self.port = util.get_unused_port()
        self.manager = util.make_manager(
            ['to_primary_q', 'from_primary_q', 
             'to_backup_q', 'from_backup_q'], self.port)
        self.to_primary_q, self.from_primary_q, \
        self.to_backup_q, self.from_backup_q = \
            self.manager.to_primary_q(), \
            self.manager.from_primary_q(), \
            self.manager.to_backup_q(), \
            self.manager.from_backup_q()

        self.message_id = 0 # id of the next message
        util.handshake(InstanceRole.CLIENT, self.port)
        self.last_health_update = time.time()

        self.stopped_flag = False
        self.received_ids = [] # ids of messages received from primary server
                               # not yet matched by messages from backup server
        self.pass_received_ids = 0 # skip putting id into received_ids
                                   # this many times

    #region UTILITY METHODS FOR COMMUNICATING WITH THE SERVERS

    def message_to_servers(self, type, body):
        """
        Send message to both primary and backup servers.
        """
        try:
            self.to_primary_q.put((self.message_id, type, body))
            #self.to_backup_q.put((self.message_id, type, body))
            self.message_id += 1
        except Exception as e:
            pass # server failure is handled elsewhere
    
    def event_to_servers(self, descr, worker=None, task = None):
        worker_id = worker.id if worker else None
        task = task if task else worker.task
        descr = f"{round(time.time()-self.begin_timestamp, 2)},{descr},{worker_id},{task.id},"  + util.tuple_to_csv(task.parameters())
        self.message_to_servers(MessageType.LOG, descr)

    #endregion COMMUNICATION WITH THE SERVERS

    #region PROCESSING MESSAGES FROM PRIMARY SERVER
 
    def process_grant_tasks(self, tasks):
        self.n_requested -= len(tasks)
        self.tasks += tasks

        if not self.titles_flag:
            # column titles
            self.titles_flag = True
            self.message_to_servers( \
                MessageType.LOG, 
                "timestamp,descr,worker_id,task_id," + \
                util.tuple_to_csv(tasks[0].parameter_titles()))
        
        for t in tasks:
            self.event_to_servers("received", task=t)
    
    def apply_domino_effect(self, hard):
        """
        1. Kills the workers that execute tasks harder than `hard`.
        2. Removes tasks harder than `hard` from self.tasks.
        """
        for w in self.workers:
            if w.killed: continue
            if w.task.hardness >= hard:
                self.event_to_servers("domino", w)
                w.my_kill()
                
        filter(lambda t: t.hardness < hard, self.tasks)

    def process_no_further_tasks(self, _body):
        self.no_further_tasks = True

    def process_stop(self, _body):
        self.stopped_flag = True

    def process_resume(self, _body):
        self.stopped_flag = False
        self.pass_received_ids = self.from_primary_q.qsize()

    def process_swap_queues(self, _body):
        self.to_primary_q, self.to_backup_q = \
            self.to_backup_q, self.to_primary_q
        self.from_primary_q, self.from_backup_q = \
            self.from_backup_q, self.from_primary_q

    def process_messages(self):
        while not self.from_primary_q.empty():
            id, type, body = self.from_primary_q.get_nowait()
            {MessageType.GRANT_TASKS: self.process_grant_tasks,
             MessageType.APPLY_DOMINO_EFFECT: self.apply_domino_effect,
             MessageType.NO_FURTHER_TASKS: self.process_no_further_tasks,
             MessageType.STOP: self.process_stop,
             MessageType.RESUME: self.process_resume,
             MessageType.SWAP_QUEUES: self.process_swap_queues,
             } \
             [type](body)
            if not self.pass_received_ids:
                self.received_ids.append(id)
            else:
                self.pass_received_ids -= 1
        
        while self.received_ids and not self.from_backup_q.empty():
            id, _type, _body = self.from_primary_q.get_nowait()
            received_id = self.received_ids.pop()
            assert(id == received_id)
    
    #endregion PROCESSING MESSAGES FROM PRIMARY SERVER

    #region PROCESSING WORKERS

    def  process_worker_started(self, worker, _body):
        self.event_to_servers("starting", worker)
    
    def  process_worker_done(self, worker, result):
        self.message_to_servers(
            MessageType.RESULT, (worker.task.id, result))
        self.event_to_servers("done", worker)

    def process_worker_messages(self):
        for w in self.workers:
            while not w.queue.empty():
                type, body = w.queue.get_nowait()
                {MessageType.WORKER_STARTED: 
                    self.process_worker_started,
                 MessageType.WORKER_DONE: self.process_worker_done,
                } [type](w, body)

    def collect_done(self):
        self.workers = list(\
            filter(lambda worker: worker.is_alive(), 
                   self.workers))

    def kill_overdue(self):
        for worker in self.workers:
            if worker.killed: continue
            before = worker.timestamp.value
            if before and time.time() - before > worker.task.timeout:
                self.event_to_servers("timeout", worker)
                worker.my_kill()
                self.message_to_servers(
                    MessageType.REPORT_HARD_TASK, 
                    worker.task.id)                

    def process_workers(self):
        self.process_worker_messages()
        self.collect_done()
        self.kill_overdue()

    #endregion PROCESSING WORKERS

    def health_update(self):
        if time.time() - self.last_health_update < \
           Constants.HEALTH_UPDATE_FREQUENCY: return
        self.message_to_servers(MessageType.HEALTH_UPDATE, None)
        self.last_health_update = time.time()

    def request_tasks(self, n: int):
        """
        Request n tasks from the server.
        """
        if n == 0: return
        self.n_requested += n
        self.message_to_servers(MessageType.REQUEST_TASKS, n)
    
    def occupy_workers(self):
        while len(self.workers) < self.capacity and self.tasks:
            task = self.tasks.pop(0)
            worker = Worker(self.next_worker_id, task, Queue())
            self.next_worker_id += 1
            self.workers.append(worker)
            worker.start()

    def run(self):
        print("Starting...", file=sys.stderr, flush=True)
        while self.tasks or not self.no_further_tasks:
            self.health_update()
            if not self.stopped_flag:
                self.process_workers()
                if not self.no_further_tasks:
                    n_tasks_in_pipeline = \
                        len(self.workers) + len(self.tasks) + self.n_requested
                    self.request_tasks(self.capacity - n_tasks_in_pipeline)
            self.process_messages()
            self.occupy_workers()
            time.sleep(Constants.CLIENT_CYCLE_WAIT)

        while self.workers:
            self.health_update()
            self.process_workers()
            time.sleep(Constants.CLIENT_CYCLE_WAIT)
                
        self.message_to_servers(MessageType.BYE, None)
        time.sleep(Constants.CLIENT_WAIT_AFTER_SENDING_BYE)
        self.manager.shutdown()