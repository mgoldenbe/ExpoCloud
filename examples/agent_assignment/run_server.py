"""
Constructs the :any:`Server` object and invokes :any:`Server.run` method.
"""

__author__ = "Meir Goldenberg"
__copyright__ = "Copyright 2022, The ExpoCloud Project"
__license__ = "MIT"
__version__ = "1.0"
__email__ = "mgoldenb@g.jct.ac.il"

class Mode:
    """
    Constant to specify whether the experiment is to be run locally or in GCE.
    """
    LOCAL = 'LOCAL'
    """
    Run the experiment locally.
    """    
    GCE = 'GCE'
    """
    Run the experiment on GCE.
    """    

if __name__ == '__main__':
    mode = Mode.LOCAL

    if mode == Mode.LOCAL:
        from src.engines.local import LocalEngine
    else:
        from src.engines.gce.engine import GCE
    from src.server import Server
    from examples.agent_assignment.instance \
        import generate_instances
    from examples.agent_assignment.task import Task
    from examples.agent_assignment.bnb import Option, Algorithm

    tasks = []
    max_n_tasks = 7
    n_instances_per_setting = 20
    
    for options in [{Option.NO_CUTOFFS}, {}, {Option.HEURISTIC}]:
        for n_tasks in range(max_n_tasks, max_n_tasks + 1):
            for n_agents in range(n_tasks, n_tasks + 1):
                instances = generate_instances(
                    n_tasks, n_agents, 
                    first_id = 0, last_id = n_instances_per_setting - 1)
                for instance in instances:
                    tasks.append(
                        Task(Algorithm(options, instance), timeout=60000))

    engine, config = None, None
    if mode == Mode.LOCAL:
        engine=LocalEngine('examples.agent_assignment')
    else:
        config = {
            'prefix': 'agent-assignment', 
            'project': 'iucc-novel-heuristic',
            'zone': 'us-central1-a',
            'server_image': 'instance-template',
            'client_image': 'instance-template',
            'root_folder': '~/ExpoCloud',
            'project_folder': 'examples.agent_assignment'
        }
        engine = GCE(config)

    Server(tasks, engine, backup = True, min_group_size=20, max_cpus_per_client=2).run()