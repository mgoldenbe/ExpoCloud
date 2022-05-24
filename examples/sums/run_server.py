from src.server import Cluster
from examples.sums.task import Task

tasks = []
for i in range(1, 11):
    tasks.append(Task(10000000 * i, 2))

Cluster(tasks).run()