from src.server import Cluster
from examples.samd.instance import generate_instances
from examples.samd.task import Task

tasks = []
max_n_tasks = 2
m_super = 7
options = {}

for n_tasks in range(2, max_n_tasks + 1):
    for n_suppliers in range(2, 9):
        instances = generate_instances(n_tasks = n_tasks, n_suppliers = n_suppliers, times_per_supplier = 4, m_super = m_super,
                                       first_id=0, last_id=19)
        for i, instance in enumerate(instances):
            tasks.append(Task(options, instance, timeout=60))

Cluster(tasks).run()