# @ Meir Goldenberg The module is part of my code for parallel execution of
# tasks on all available CPUs. Features of the code:
# - Timeout of tasks
# - Remembers hard tasks
#   - Avoid starting a hard task based on history
#   - Whenever a task is killed, kill other hard tasks currently running
# - Can stop Google Compute Engine instance upon completion

import multiprocessing as mp
import time
import sys
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

class Cluster():
    """

    """

    def __init__(self, tasks, timeout = 1000000):
        self.cycle = 0.01 # waiting time between pings of workers
        self.cycle_number = 0
        self.workers = []
        self.id = 0 # next worker id
        self.hard = [] # hard tasks, i.e. tasks that were overdue

        # Store original order for results output, then sort by difficulty
        for i, t in enumerate(tasks): 
            t.orig_id = i
        self.tasks = sorted(tasks)

        self.tasks_dict = {}
        for i, t in enumerate(self.tasks):
            t.id = i
            t.result = None
            if t.parameters() not in self.tasks_dict:
                t.param_id = 0
                self.tasks_dict[t.parameters()] = [t]
            else:
                t.param_id = len(self.tasks_dict[t.parameters()])
                self.tasks_dict[t.parameters()].append(t)
        
        # column titles
        print("timestamp,descr,worker_id,task_id,param_id," + \
              util.tuple_to_csv(tasks[0].parameter_titles()),
              file=sys.stderr, flush=True)
        # dump tasks
        for t in self.tasks:
            util.print_event("submitted", task=t)
        
        #self.tasks.reverse() # since we use pop() to get the new task
        self.timeout = timeout
        self.results = mp.Queue()
        self.capacity = mp.cpu_count()

    def collect_done(self):
        self.workers = list(\
            filter(lambda worker: worker.is_alive(), self.workers))

    def kill_overdue(self):
        for worker in self.workers:
            if worker.killed: continue
            before = worker.timestamp.value
            if before and time.time() - before > self.timeout:
                util.print_event("timeout", worker)
                self.hard.append(worker.task)
                worker.my_kill()
                # now kill all tasks as hard or harder than this one
                for w in self.workers:
                    if w.killed: continue
                    if w.task >= worker.task:
                        util.print_event("domino", w)
                        w.my_kill()

    def process_workers(self):
        self.kill_overdue()
        self.collect_done()

    def is_task_hard(self, task):
        for h in self.hard:
            if task >= h: return True
        return False

    def occupy_workers(self):
        global id
        while len(self.workers) < self.capacity and self.tasks:
            task = self.tasks.pop()
            if self.is_task_hard(task):
                util.print_event("not starting", task = task)
                time.sleep(0.1)
                continue
            self.id += 1
            worker = Worker(self.id, task, self.results)
            self.workers.append(worker)
            worker.start()

    def run(self):
        while self.tasks:
            self.cycle_number += 1
            self.process_workers()
            self.occupy_workers()
            time.sleep(self.cycle)

        while self.workers:
            self.cycle_number += 1
            self.process_workers()
            time.sleep(self.cycle)
        
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