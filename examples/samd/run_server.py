from src.engines.local import LocalEngine
from src.engines.gce.engine import GCE
from src.server import Cluster
from examples.samd.instance import generate_instances
from examples.samd.task import Task

tasks = []
max_n_tasks = 8
m_super = 7
options = ()

for n_tasks in range(max_n_tasks, max_n_tasks + 1):
    for n_suppliers in range(2, 3):
        instances = generate_instances(n_tasks = n_tasks, n_suppliers = n_suppliers, times_per_supplier = 4, m_super = m_super,
                                       first_id=0, last_id=19)
        for i, instance in enumerate(instances):
            tasks.append(Task(options, instance, timeout=120))

config = {
    'prefix': 'samd', 
    'project': 'iucc-novel-heuristic',
    'zone': 'us-central1-a',
    'server_image': 'samd-server',
    'client_image': 'client-template',
    'root_folder': '~/ExpoCloud',
    'project_folder': 'examples.samd'
}
engine = GCE(config)
#engine=LocalEngine('examples.samd')
Cluster(tasks, engine, min_group_size=20).run()