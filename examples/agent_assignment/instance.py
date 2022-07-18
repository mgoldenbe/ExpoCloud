import random

class Task:
    def __init__(self, id, agent_costs):
        self.id = id
        self.agent_costs = agent_costs
    
    def id(self): return self._id
    
    def __str__(self):
        return f"Task {self.id}: {self.agent_costs}"

class Instance:
    def __init__(self, i, tasks):
        assert(len(tasks) > 0)
        self.id = i
        self.tasks = tasks
        self.n_tasks = len(self.tasks)
        self.n_agents = len(self.tasks[0].agent_costs)
    
    def parameter_titles(self):
        return ("id", "|T|", "|A|")

    # Output id, number of tasks, number of suppliers per task, 
    # number of times per supplier, m_super
    def parameters(self):
        return (self.id, self.n_tasks, self.n_agents)
        
    def __str__(self):
        result = f"Instance {self.id}\n"
        result += "\n" + '\n'.join([str(t) for t in self.tasks])  + "\n"
        result += '\n\n------------------------------------------------------\n'
        return result

# returns list of tuples (id, tasks)
# instance with same id is always same
def generate_instances(n_tasks, n_agents, first_id, last_id):
    assert(n_agents >= n_tasks)
    random.seed(47)
    instances = []
    for i in range(last_id + 1):
        instance = Instance(i, [Task(id, [random.randint(1,100) for _ in range(n_agents)]) for id in range(n_tasks)])
        if i >= first_id: 
            instances.append(instance)
    return instances