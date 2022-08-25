"""
The two classes implementing the client - the ``Worker`` and the ``Client``.
"""

__author__ = "Meir Goldenberg"
__copyright__ = "Copyright 2022, The ExpoCloud Project"
__license__ = "MIT"
__version__ = "1.0"
__email__ = "mgoldenb@g.jct.ac.il"

from multiprocessing import Process, Value, Queue, cpu_count
import time
import socket
from typing import Any, List

from src.abstract_task import AbstractTask

from src.util import my_print
from src.constants import Verbosity

from src import util
from src.util import InstanceRole, MessageType
from src.constants import Constants

# Responsible for a single task
class Worker(Process):
    """
    The class representing a worker charged with a task.
    """    
    def __init__(self, id: int, task: AbstractTask, queue: Queue):
        """
        The constructor.

        :param id: The worker's id.
        :type id: int
        :param task: The task to execute.
        :type task: AbstractTask
        :param queue: The queue for communicating the result.
        :type queue: Queue
        """        
        Process.__init__(self)
        self.id = id
        self.task = task
        self.queue = queue
        self.timestamp = Value('d', 0)
        self.killed = False # takes time after kill() before is_alive() == False

    def my_kill(self):
        """
        Terminate the worker.
        """        
        self.killed = True
        self.kill()

    def run(self):
        """
        Run the worker process executing the task.
        """        
        my_print(Verbosity.workers, f"Worker {self.id} with {self.task.id}")
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
        The constructor. In particular, it shakes hands with the primary server and create the queues for communicating with both the primary and the backup servers.
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
        self.capacity = min(cpu_count(), util.command_arg_max_cpus())

        self.port_primary = util.get_unused_port()
        self.manager_primary = util.make_manager(
            ['outbound', 'inbound'], self.port_primary)
        self.port_backup = util.get_unused_port()
        self.manager_backup = util.make_manager(
            ['outbound', 'inbound'], self.port_backup)

        self.to_primary_q, self.from_primary_q, \
        self.to_backup_q, self.from_backup_q = \
            self.manager_primary.outbound(), \
            self.manager_primary.inbound(), \
            self.manager_backup.outbound(), \
            self.manager_backup.inbound()

        self.message_id = 0 # id of the next message
        util.handshake(
            InstanceRole.CLIENT, self.port_primary, self.port_backup)
        self.last_health_update = time.time()

        self.stopped_flag = False
        self.received_ids = [] # ids of messages received from primary server
                               # not yet matched by messages from backup server
        
        # When RESUME is received, the backup server will never match the messages currently in from_primary_q. Hence, when these messages are processed, their ids should not be stored. The following variable remembers the number of such messages.
        self.pass_received_ids = 0

    #region UTILITY METHODS FOR COMMUNICATING WITH THE SERVERS

    def message_to_servers(self, type: MessageType, body: Any):
        """
        Send the message to both primary and backup servers.

        :param type: The message type.
        :type type: MessageType
        :param body: The body of the message.
        :type body: Any
        """        
        try:
            self.to_primary_q.put((self.message_id, type, body))
            self.to_backup_q.put((self.message_id, type, body))
            self.message_id += 1
        except Exception as e:
            pass # server failure is handled elsewhere
    
    def event_to_servers(
        self, descr: str, worker: Worker = None, task: AbstractTask = None):
        """
        Send an event to servers.

        :param descr: The event description.
        :type descr: str
        :param worker: The worker at which the event took place or ``None`` if the event is not related to any worker; defaults to None.
        :type worker: Worker, optional
        :param task: The task to which the event is relevant or ``None`` if the event is not relevant to any task; defaults to ``None``.
        :type task: AbstractTask, optional
        """        
        worker_id = worker.id if worker else None
        task = task if task else worker.task
        descr = f"{round(time.time()-self.begin_timestamp, 2)},{descr},{worker_id},{task.id},"  + util.tuple_to_csv(task.parameters())
        self.message_to_servers(MessageType.LOG, descr)

    #endregion COMMUNICATION WITH THE SERVERS

    #region PROCESSING MESSAGES FROM PRIMARY SERVER
 
    def process_grant_tasks(self, tasks: List[AbstractTask]):
        """
        Process tasks granted to this client by the primary server.

        :param tasks: The tasks granted.
        :type tasks: List[AbstractTask]
        """        
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
    
    def apply_domino_effect(self, hard: tuple):
        """
        Apply the domino effect based on the given hardness. Namely,

        * Terminate the workers that execute tasks with hardness greater than or equal to :paramref:`hard`.
        * Remove the corresponding tasks from ``self.tasks``.

        :param hard: The hardness of the task that timed out as reported by the primary server.
        :type hard: tuple
        """        
        for w in self.workers:
            if w.killed: continue
            if w.task.hardness >= hard:
                self.event_to_servers("domino", w)
                w.my_kill()
                
        filter(lambda t: t.hardness < hard, self.tasks)

    def process_no_further_tasks(self, _body):
        """
        Set the flag about having receive the ``NO_FURTHER_TASKS`` message from the primary server.
        """        
        self.no_further_tasks = True

    def process_stop(self, _body):
        """
        Set the flag about having receive the ``STOP`` message from the primary server.
        """
        self.stopped_flag = True

    def process_resume(self, _body):
        """
        Unset the flag about having receive the ``STOP`` message from the primary server and perform the relevant bookkeeping.
        """
        self.stopped_flag = False
        my_print(Verbosity.message_sync,
                f"Removing received_ids: {self.received_ids}")
        self.received_ids = []
        assert(self.pass_received_ids == 0)
        self.pass_received_ids = self.from_primary_q.qsize()
        my_print(Verbosity.message_sync,
                f"Will pass {self.pass_received_ids} ids: {[self.from_primary_q.queue[i][0] for i in range(self.pass_received_ids)]}")

    def process_swap_queues(self, _body):
        """
        Swap the queues for communication with the servers, so that the queues previously used for communication with the backup server are now treated as the queues for communication with the primary server, and vice versa.
        """        
        self.to_primary_q, self.to_backup_q = \
            self.to_backup_q, self.to_primary_q
        self.from_primary_q, self.from_backup_q = \
            self.from_backup_q, self.from_primary_q
        
        self.port_primary, self.port_backup = \
            self.port_backup, self.port_primary
        self.manager_primary, self.manager_backup = \
            self.manager_backup, self.manager_primary

    def process_messages(self):
        """
        Process messages from the servers.
        """        
        while not self.from_primary_q.empty():
            id, type, body = self.from_primary_q.get_nowait()
            my_print(Verbosity.messages, f"Primary server sent {id} {type}")
            {MessageType.GRANT_TASKS: self.process_grant_tasks,
             MessageType.APPLY_DOMINO_EFFECT: self.apply_domino_effect,
             MessageType.NO_FURTHER_TASKS: self.process_no_further_tasks,
             MessageType.STOP: self.process_stop,
             MessageType.RESUME: self.process_resume,
             MessageType.SWAP_QUEUES: self.process_swap_queues,
             } \
             [type](body)

            assert(self.pass_received_ids >= 0)
            if id:
                if self.pass_received_ids:
                    my_print(Verbosity.message_sync,
                            f"Not appending into received_ids (self.pass_received_ids={self.pass_received_ids})")
                    self.pass_received_ids -= 1
                else:
                    self.received_ids.append(id)                
        
        while self.received_ids and not self.from_backup_q.empty():
            id, type, body = self.from_backup_q.get_nowait()
            my_print(Verbosity.messages, 
                    f"Backup server sent {id} {type}: {body}")
            received_id = self.received_ids.pop(0)
            my_print(Verbosity.message_sync, 
                    f"Primary server had sent received_id={received_id}")
            assert(id == received_id)
    
    #endregion PROCESSING MESSAGES FROM PRIMARY SERVER

    #region PROCESSING WORKERS

    def  process_worker_started(self, worker: Worker, _body):
        """
        Process the event of the worker having started by sending the corresponding event to the servers.

        :param worker: The worker that started.
        :type worker: Worker
        """        
        self.event_to_servers("starting", worker)
    
    def  process_worker_done(self, worker: Worker, result: tuple):
        """
        Process the event of the worker having completed its task by sending both the result and the corresponding event to the servers.

        :param worker: The worker that finished.
        :type worker: Worker
        :param worker: The result for the completed task.
        :type worker: tuple
        """
        self.message_to_servers(
            MessageType.RESULT, (worker.task.id, result))
        self.event_to_servers("done", worker)

    def process_worker_messages(self):
        """
        Process messages from workers.
        """        
        for w in self.workers:
            while not w.queue.empty():
                type, body = w.queue.get_nowait()
                {MessageType.WORKER_STARTED: 
                    self.process_worker_started,
                 MessageType.WORKER_DONE: self.process_worker_done,
                } [type](w, body)

    def collect_garbage(self):
        """
        Remove objects corresponding to workers that have terminated.
        """        
        self.workers = list(\
            filter(lambda worker: worker.is_alive(), 
                   self.workers))

    def kill_overdue(self):
        """
        Terminate workers whose task has timed out. For each timed out task, send the ``REPORT_HARD_TASK`` message to the servers.
        """        
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
        """
        Manage the workers by performing three actions:

        * Process messages from the workers.
        * Remove objects corresponding to workers that have terminated.
        * Process workers whose task has timed out.
        """        
        self.process_worker_messages()
        self.collect_garbage()
        self.kill_overdue()

    #endregion PROCESSING WORKERS

    def health_update(self):
        """
        Send health update to the servers.
        """        
        if time.time() - self.last_health_update < \
           Constants.HEALTH_UPDATE_FREQUENCY: return
        self.message_to_servers(MessageType.HEALTH_UPDATE, None)
        self.last_health_update = time.time()

    def request_tasks(self, n: int):
        """
        Send a message to the servers requesting :paramref:`n` tasks.

        :param n: The number of tasks to request.
        :type n: int
        """        
        
        if n == 0: return
        self.n_requested += n
        self.message_to_servers(MessageType.REQUEST_TASKS, n)
    
    def occupy_workers(self):
        """
        Create workers for the tasks assigned to this client, charge the new workers with tasks and start them.
        """        
        while len(self.workers) < self.capacity and self.tasks:
            task = self.tasks.pop(0)
            worker = Worker(self.next_worker_id, task, Queue())
            self.next_worker_id += 1
            self.workers.append(worker)
            worker.start()

    def run(self):
        """
        The main loop of the client. The loop is over when there are no more tasks to be assigned to the client and all tasks previously assigned to it are completed. Once the loop is over, send the ``BYE`` message to the servers, wait for them to process this message, and complete.
        """        
        my_print(Verbosity.all, "Starting...")
        while self.tasks or not self.no_further_tasks or self.workers:
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

        my_print(Verbosity.all, "Sending BYE")       
        self.message_to_servers(MessageType.BYE, None)
        time.sleep(Constants.CLIENT_WAIT_AFTER_SENDING_BYE) 
        self.manager_primary.shutdown()
        self.manager_backup.shutdown()
        my_print(Verbosity.all, "Done")