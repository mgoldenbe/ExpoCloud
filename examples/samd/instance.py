import sys
sys.path.append('examples/samd/legacy/') # so imports in the imported modules shouldn't fail
from examples.samd.legacy.AStar_vs_huristcs_appx import *
from examples.samd.legacy.Mesa import KolmogorovEfficientApproxOneSideOn2
from examples.samd.legacy.OptTrim import optTrim
import random

class Distribution:    
    def __init__(self, raw):
        self._raw = raw
    
    def support_size(self): return len(self._raw)
    
    def __add__(self, other):
        return Distribution(sumRandVars(self._raw, other._raw))
    
    def approx(self, m):
        #print(f"Approximating, support={self.support_size()}")
        return Distribution(KolmogorovEfficientApproxOneSideOn2(self._raw, m))
    
    def add_approx(self, other, m):
        try:
            return Distribution(sumRandVarsAppx(self._raw, other._raw, m))
        except:
            print("Add approx exception")
            print(self._raw)
            print(other._raw)
    
    def add(self, other, m = -1):
        if m == -1: return self + other
        return self.add_approx(other, m)
    
    def prob_before_deadline(self, deadline):
        return lessThanT(self._raw, deadline)
    
    def __str__(self):
        return ' | '.join(['%.2fs: p=%.3f' % (time, prob) 
                         for (time, prob) in sorted(self._raw.items())])

class Supplier:
    def __init__(self, name, distr):
        self._name = name
        self._distr = distr
    
    def name(self): return self._name
    def distr(self): return self._distr
    def support_size(self): return self.distr().support_size()
    
    def __str__(self):
        return f"Supplier {self._name}   {self._distr}"

# List of suppliers to string
def assignment2str(assignment):
    return ';'.join([s.name() for s in assignment])

class Task:
    def __init__(self, task_id):
        self._id = task_id
        self._suppliers = []
    
    def id(self): return self._id
    def suppliers(self): return self._suppliers
    
    def add_supplier(self, supplier):
        self._suppliers += [supplier]
    
    def parameter_titles(self):
        return ("n_sup", "spprt")

    def parameters(self):
        return (len(self._suppliers), self._suppliers[0].support_size())
    
    # number of suppliers, number of times per supplier
    def __str__(self):
        return f"Task {self._id}\n" + '\n'.join([f"    {s}" for s in self._suppliers])

class Instance:
    # tasks_super are the tasks, with only the super-supplier for each task.
    # time_super is the time taken to compute super-suppliers.
    def __init__(self, graph, tasks, tasks_super, m_super, time_super):
        self.graph = graph
        self.tasks = tasks
        self.tasks_super = tasks_super
        self.m_super = m_super
        self.time_super = time_super
        self.deadline = 0.35 * (len(tasks) - 1) * tasks[0].suppliers()[0].support_size()
    
    def assign_id(self, i):
        self.id = i
    
    def parameter_titles(self):
        return ("id", "ddln", "|T|") + \
               self.tasks[0].parameter_titles() + ("m_spr",)

    # Output id, number of tasks, number of suppliers per task, 
    # number of times per supplier, m_super
    def parameters(self):
        return (self.id, self.deadline, len(self.tasks)) + self.tasks[0].parameters() + (self.m_super,)
        
    def __str__(self):
        result = f"Instance {self.id}\n"
        result += "\n" + '\n'.join([str(t) for t in self.tasks])  + "\n"
        result += f"\nSuper-suppliers (time taken: {self.time_super:.3f}s):\n"
        result += '\n'.join([str(t) for t in self.tasks_super])
        result += '\n\n------------------------------------------------------\n'
        return result
        
# Assumes that the start node is 's' and the goal node is 'g'
def graph_to_instance(graph, m_super, i):
    tasks = []
    begin = time.time()
    raw_super = preProcessMinProb(graph,'s','g',m_super, 0)[0]
    #if i == 2103: print(raw_super)
    time_result = time.time() - begin
    tasks_super = []
    node_name = 's'
    task_id = 0
    while node_name != 'g':
        t = Task(task_id)
        t_super = Task(task_id)
        t_super.add_supplier(Supplier(str(task_id) + "_super", Distribution(raw_super[node_name])))
        for name, distr in graph[node_name].items():
            t.add_supplier(Supplier(name, Distribution(distr)))
        tasks.append(t)
        tasks_super.append(t_super)
        task_id += 1
        node_name = name
    return Instance(graph, tasks[:-1], tasks_super[:-1], m_super, time_result) # -1, since without g

# returns list of tuples (id, tasks, tasks_super)
# instance with same id is always same
# Note! Number of layers is smaller by one than the number of tasks.
def generate_instances(n_tasks, n_suppliers, times_per_supplier, m_super,
                       first_id, last_id):
    random.seed(47)
    instances = []
    for i in range(last_id + 1):
        instance = graph_to_instance(
            creatGraph(n_suppliers, n_tasks - 1, times_per_supplier, 0), m_super, i)
        instance.assign_id(i)
        if i >= first_id: 
            instances.append(instance)
    return instances