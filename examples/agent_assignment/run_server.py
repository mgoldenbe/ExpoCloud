from src.engines.local import LocalEngine
#from src.engines.gce.engine import GCE
from src.server import Server
from examples.agent_assignment.instance import generate_instances
from examples.agent_assignment.task import Task
from examples.agent_assignment.bnb import Option

tasks = []
max_n_tasks = 8
options = {Option.HEURISTIC}

for n_tasks in range(max_n_tasks, max_n_tasks + 1):
    for n_agents in range(n_tasks, n_tasks + 1):
        instances = generate_instances(
            n_tasks, n_agents, first_id=0, last_id=19)
        for i, instance in enumerate(instances):
            tasks.append(Task(options, instance, timeout=60))

config = {
    'prefix': 'samd', 
    'project': 'iucc-novel-heuristic',
    'zone': 'us-central1-a',
    'server_image': 'samd-server',
    'client_image': 'client-template',
    'root_folder': '~/ExpoCloud',
    'project_folder': 'examples.samd'
}
#engine = GCE(config)
engine=LocalEngine('examples.agent_assignment')
Server(tasks, engine, backup=False, min_group_size=20).run()