# @ Meir Goldenberg The module is part of the ExpoCloud Framework

import multiprocessing as mp
from multiprocessing.managers import SyncManager

import time
import sys
import socket
from . import util

class AbstractTask:
    """
    This is a parent class for Task classes.
    """

    def parameter_titles(self):
        """
        Returns the tuples of titles of columns for parameters for formatted output.
        Global id and id per parameter setting are included by default and should not appear here.
        """
        return ()

    def parameters(self):
        """
        Returns the tuple of parameters. These parameters are used by the cluster:
        1. To determine the instance number for each parameter setting.
        2. To provide formatted output.
        Global id and id per parameter setting are included by default and should not appear here.
        """
        return ()
    
    def result_titles(self):
        """
        Returns the tuples of titles of columns for results for formatted output.
        """
        return ()

    def __lt__(self, other):
        """
        Returns true if this task is strictly easier than the `other` task.
        The following implementation relies on the existence of the hardness_parameters() function.
        """
        return util.all_lt(self.hardness_parameters(), 
                           other.hardness_parameters())
    
    def __le__(self, other):
        """
        Returns true if this task is easier than or of the same hardness as the `other` task.
        The following implementation relies on the existence of the hardness_parameters() function.
        """
        return util.all_le(self.hardness_parameters(), 
                           other.hardness_parameters())
        

# Responsible for a single task
class Worker(mp.Process):
    def __init__(self, id, task, results):
        mp.Process.__init__(self)
        self.id = id
        self.task = task
        self._results = results
        self.timestamp = mp.Value('d', 0)
        self.killed = None

    def my_kill(self):
        self.killed = True
        self.kill()

    def run(self):
        self.timestamp.value = time.time()
        util.print_event("starting", self)
        self.task.result = self.task.run() # task should handle exceptions
        util.print_event("done", self)
        self._results.put(self.task)

class Client:
    """
    The main client class.
    """

    def __init__(self):
        """
        Perform handshake with the server and create the queues to ask for tasks and to get them. 
        """
        self.cycle_wait = 0.01 # waiting time between cycles
        self.response_wait = 1 # waiting time for response from the server
        self.workers = []
        self.id = 0 # next worker id
        self.name = socket.gethostname()
        self.titles_flag = False # whether have already output column titles
        self.done_flag = False # True if there are no more tasks at the server
        self.results = mp.Queue()
        self.capacity = mp.cpu_count()

        # Now perform the handshake with the server
        server_ip = sys.argv[1]
        server_port = sys.argv[2]
        auth = b'myauth'

        class ServerQueueManager(SyncManager):
            pass

        ServerQueueManager.register('handshake_q')
        ServerQueueManager.register('hard_q')

        request_tasks_q_name = f"request_tasks_{self.name}_q"
        grant_tasks_q_name = f"grant_tasks_{self.name}_q"
        ServerQueueManager.register(request_tasks_q_name)
        ServerQueueManager.register(grant_tasks_q_name)

        try:
            manager = ServerQueueManager( \
                address=(server_ip, server_port), authkey=auth)
            manager.connect()
        except:
            print("Connection to server failed", file=sys.stderr, flush=True)
            exit(1)

        try:
            handshake_q = manager.handshake_q()
            handshake_q.put(self.name)
            time.sleep(self.response_wait)
            self.hard_q = manager.hard_q()
            self.request_tasks_q = getattr(manager, request_tasks_q_name)()
            self.grant_tasks_q = getattr(manager, grant_tasks_q_name)()
        except:
            print("Handshake with the server failed", 
                  file=sys.stderr, flush=True)
            exit(1)

    def get_tasks(self, n: int) -> list[AbstractTask]:
        """
        Request n tasks from the server.
        Return the list of tasks.
        """
        try:
            self.request_tasks_q.put(n)
            time.sleep(self.response_wait)
            tasks = self.grant_tasks_q.get_nowait()
        except:
            print("Getting tasks failed", file=sys.stderr, flush=True)
            exit(1)
        
        if len(tasks) < n:
            self.done_flag = True

        if not tasks: 
            return []

        if not self.titles_flag:
            # column titles
            print("timestamp,descr,worker_id,task_id,param_id," + \
                  util.tuple_to_csv(tasks[0].parameter_titles()),
                  file=sys.stderr, flush=True)
        
        for t in tasks:
            util.print_event("received", task=t)
        
        return tasks

    def collect_done(self):
        self.workers = list(\
            filter(lambda worker: worker.is_alive(), self.workers))

    def domino_effect(self, killed_worker):
        """
        Kill all tasks as hard or harder than this one and report to server.
        """
        for w in self.workers:
            if w.killed: continue
            if w.task >= killed_worker.task:
                util.print_event("domino", w)
                w.my_kill()
        self.hard_q.put(killed_worker.task)
        
    def kill_overdue(self):
        for worker in self.workers:
            if worker.killed: continue
            before = worker.timestamp.value
            if before and time.time() - before > self.timeout:
                util.print_event("timeout", worker)
                self.hard.append(worker.task)
                worker.my_kill()
                self.domino_effect(worker)                

    def process_workers(self):
        self.kill_overdue()
        self.collect_done()

    def occupy_workers(self, tasks):
        global id
        while len(self.workers) < self.capacity and tasks:
            task = tasks.pop(0)
            if self.is_task_hard(task):
                util.print_event("not starting", task = task)
                time.sleep(0.1)
                continue
            self.id += 1
            worker = Worker(self.id, task, self.results)
            self.workers.append(worker)
            worker.start()

    def run(self):
        while not self.done_flag:
            self.process_workers()
            n_free_workers = self.capacity - len(self.workers)
            if n_free_workers > 0:
                tasks += self.get_tasks(n_free_workers)
                self.occupy_workers(tasks)
            time.sleep(self.cycle_wait)

        while self.workers:
            self.process_workers()
            time.sleep(self.cycle_wait)
        
        done_tasks = []
        while not self.results.empty():
            done_tasks.append(self.results.get())

        # TODO: form dictionary parameters => number of done tasks
        # Filter done tasks to include only those with enough done tasks with the same parameters.
        # See if tasks_dict is used.

        # restore order
        done_tasks.sort(key = lambda t: t.orig_id)
        if done_tasks:
            print(util.tuple_to_csv(done_tasks[0].parameter_titles() + \
                                    done_tasks[0].result_titles()))
            for t in done_tasks:
                print(util.tuple_to_csv(t.parameters() + t.result))