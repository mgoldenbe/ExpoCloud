# ExpoCloud: a Framework for Time and Budget-Effective Parameter Space Explorations using a Cloud Compute Engine

Large parameter space explorations are some of the most time consuming yet critically important tasks in many fields of modern scientific research. ExpoCloud enables the researcher to harness cloud compute resources to achieve time and budget-effective concurrent large-scale parameter space explorations. 

ExpoCloud enables maximal possible levels of concurrency by on-demand creation of compute instances, saves money by terminating unneeded instances, provides a mechanism for saving both time and money by avoiding the exploration of parameter settings that are as hard or harder than the parameter settings that timed out. Effective fault tolerance mechanisms make ExpoCloud suitable for large experiments.

ExpoCloud supports various cloud environments through extension classes. There is currently a class to support the Google Compute Engine. There is also a class that simulates a cloud environment on the local machine. This makes facilitates further development by making it easy to debug the system.

The reminder of this page gives an example of usage. The developer documentation is provided at https://expocloud.netlify.app.

This user documentation supplements the article submitted to the *Journal of Parallel and Distributed Computing*. Should the article be accepted, the link to it will appear here.

## The example experiment: solving the Agent Assignment problem using Branch and Bound
Consider the agent assignment problem, as follows. There are *n* agents
and *m* tasks, such that *n* ≥ *m*. For each agent *i* and task *j*, we
are given *t*<sub>*ij*</sub>, the amount of time, in seconds, that the
agent *i* expends to complete the task *j*. We need to assign an agent
to each task, such that no agent is assigned to more than one task and
the total time of completing all the tasks is minimized.

Suppose we use the classical branch and bound (B&B) search algorithm for
solving this problem, as follows. The possible assignments of agents to
tasks are searched recursively, so that only the current partial
assignment is kept in memory. At all times, we keep the currently best
full assignment and the corresponding time. Whenever the time
corresponding to the current partial assignment is greater than that of
the best full assignment, the current assignment is not extended. We say
that this branch of the search is cut off. When such cutoffs are
disabled, we get a brute-force algorithm.

A more efficient version of this algorithm uses a heuristic. Given a
partial assignment, the heuristic is a lower bound on the time needed to
complete the remaining tasks it computed. This bound is computed by
assigning the best of the unused agents to each of the remaining tasks,
while allowing the assignment of the same remaining agent to more than
one remaining task. Whenever the sum of the time corresponding to the
current partial assignment and the heuristic is greater than that of the
best full assignment, the current assignment is not extended.

Assuming that we need to explore the effectiveness of the algorithm when
task completion times of the agents come from different random
distributions, we have the following parameters:

1.  The distribution of task completion times *D*,

2.  The number of agents *n*,

3.  The number of tasks *m*, and

4.  The algorithm variant (either `no cutoffs`, or `classical B&B`, or
    `B&B with heuristic`).

To thoroughly understand the properties of the problem at hand and the
performance characteristics of the search algorithm, we need to generate
a number of problem instances, i.e. the times *t*<sub>*ij*</sub> taken
by agent *i* to complete the assignment *j*, for each setting of the
*D*, *n* and *m*. We then need to run each algorithmic variant to solve
each of these instances. At each run, we need to collect both the
results (i.e. the best full agent assignment and time) and a number of
performance characteristics, such as run-time and the number of
evaluated partial assignments.

## Running the example experiment
The implementation of the example experiment is located in `examples/agent_assignment/`. In this section, you will see how to run this experiment. In the following sections, you will see how this experiment is implemented.

### Running the example experiment locally
Let us begin by running the experiment on your local machine. To do that, you need to make sure that the line assigning the `mode` variable in the `run_server` script reads:

```python
mode = Mode.LOCAL
```

Make sure that you are in the main ExpoCloud directory and run:

```sh
python -m examples.agent_assignment.run_server
```

That is it! The experiment will run automatically until all tasks are executed. Now, let us see what output we get. First, there is the output in the terminal (which you can redirect, of course), which shows you how the experiment progressed - what instances were created, what tasks were completed etc. The level of verbosity of this output can be controlled by modifying the constants in the `Verbosity` class in `src/constants.py`. Then, there are the files whose names begin with `out-` and `err-` that contain the `stdout` and `stderr`, respectively, of the instances other than the original primary server.

Lastly, and most importantly, there are the directories whose names begin with `output-`. These correspond to the servers. Inside such a directory, there is the `results.txt` file containing the results of the executed tasks, which we can view in the tabular format using the provided `view.sh` script, e.g.:

```sh
./view.sh output-server-1/results.txt
```

The server directories contain also a folder for each client. Such a folder contains two files: `events.txt` and `exceptions.txt`, which contain the events related to the computation and the exceptions, respectively, sent by the client. The contents of `events.txt` is best viewed in the tabular form using the `view.sh` script as demonstrated above.

You can simulate a failure of an instance by simply killing the corresponding process. For example, here is a simulation of the primary server failure:

```sh
pkill -9 -f run_server
```

In this case, there will be directories whose names begin with `output-` corresponding to the replacement servers.

### Running the example experiment on the cloud
Let us run the above experiment on the Google Compute Engine. Make sure that the line assigning the `mode` variable in the `run_server` script reads:

```python
mode = Mode.GCE
```

To run the experiment, one needs to first create machine images to serve as prototypes for all future servers and clients. The content for these images is same - the ExpoCloud folder in the home directory. For the machine configuration, a server needs a single CPU and it does not need to be strong one. Thus, the cheapest configuration available on a modern cloud service should suffice. For debugging purposes, the same machine image can be used for the client instances. For production, multiple strong CPUs might be required. Luckily, ExpoCloud takes care of deleting the client instances as soon as they are not needed.

The experiment is run by the following command issued on the primary server instance while in the main ExpoCloud folder:

```sh
python -m examples.agent_assignment.run_server
```

The folder whose name begins with `output-` will contain the results and the information about the events sent by the clients just like for the locally executed experiment. Should the primary server fail, the results will be stored on the replacement server.

In contrast to the local setting, the `run_server` script does not terminate. This is done so that, should the primary server fail after the results have been produced, the results should not be lost.

## The classes provided by the user
The user needs to define what a task is. Here is the definition for the example project contained in `examples/agent_assignment/task.py`:

```python
class Task(AbstractTask):
    def __init__(self, algorithm: Algorithm, timeout = 60):
        """
        The constructor.

        :param algorithm: The algorithm used to solve the problem instance.
        :type algorithm: Algorithm
        :param timeout: The deadline for the task in seconds.
        :type timeout: float
        """
        super(Task, self).__init__(algorithm, timeout)

    def group_parameter_titles(self) -> Tuple[str]:
        """
        Return the tuple of names of parameters that determine groups for counting the number of non-hard instances.

        :return: The tuple of names of parameters that determine groups for counting the number of non-hard instances.
        :rtype: Tuple[str]
        """
        return filter_out(self.parameter_titles(), ('id',))

    def parameter_titles(self) -> Tuple[str]:
        """
        Return the tuple of names of parameters that characterize the task.

        :return: The tuple of names of parameters that characterize the task.
        :rtype: Tuple[str]
        """
        return self.instance.parameter_titles() + ("Options",)
        
    def parameters(self) -> tuple:
        """
        Return the tuple of parameters that characterize the task.

        :return: The tuple of parameters that characterize the task.
        :rtype: tuple
        """
        return self.instance.parameters() + (set2str(self.options),)

    def hardness_parameters(self) -> tuple:
        """
        Return the tuple of parameters determining the hardness of the task. This is to be used to initialize the Hardness object.

        :return: The tuple of parameters determining the hardness of the task. 
        :rtype: tuple
        """        
        return (self.instance.n_tasks, self.instance.n_agents)

    def result_titles(self) -> Tuple[str]:
        """
        Return the tuple of names of output values for the solved task.

        :return: The tuple of names of output values for the solved task.
        :rtype: Tuple[str]
        """
        return self.algorithm.result_titles()

    def run(self) -> tuple:
        """
        Return the tuple of output values for the solved task.

        :return: The tuple of output values for the solved task.
        :rtype: tuple
        """
        return self.algorithm.search()
```

Note that all such ``Task`` classes should be derived from the [AbstractTask](https://expocloud.netlify.app/src.html#module-src.abstract_task) class. The methods of the above ``Task`` class are described [here](https://expocloud.netlify.app/examples.agent_assignment.html#module-examples.agent_assignment.task).

Most of the work is delegated to the methods of the [Algorithm](https://expocloud.netlify.app/examples.agent_assignment.html#examples.agent_assignment.bnb.Algorithm) class implementing the Branch and Bound search algorithm and the [Instance](https://expocloud.netlify.app/examples.agent_assignment.html#module-examples.agent_assignment.instance) class for representing an instance of the agent assignment problem. 

Now let us look at the scripts that the user need to write for running the first primary server and the script for running a client that the user needs to write.

## The `run_server` script
The `run_server` script has to:

1. Generate the list of tasks to be executed.
2. Construct the object representing the compute engine.
3. Construct the primary server object, passing to it the list of tasks, the engine and, possibly, other optional arguments.
4. Run the primary server by invoking the `run` method.

For simplicity, the example script fixes the distribution *D* of times *t*<sub>*ij*</sub> used by agent *i* to solve task *j* is fixed. We the import statements and the assignment of `mode` and get right to the construction of the list of tasks:

```python
tasks = []
max_n_tasks = 50
n_instances_per_setting = 20

for options in [{Option.NO_CUTOFFS}, {}, {Option.HEURISTIC}]:
    for n_tasks in range(2, max_n_tasks + 1):
        for n_agents in range(n_tasks, 2 * n_tasks):
            instances = generate_instances(
                n_tasks, n_agents, 
                first_id = 0, last_id = n_instances_per_setting - 1)
            for instance in instances:
                tasks.append(
                    Task(Algorithm(options, instance), timeout=60000))
```

The body of the outer loop is executed for each of the three variants of the algorithm: brute-force, classic B&B and B&B with a heuristic. The rest of this code is self-explanatory. As a result of this nested loop, the generated list of tasks is stored in the variable `tasks`.

The next section of the script specifies the configuration for the
compute engine and passes this configuration to the constructor of the
engine object:

```python
config = {
    'prefix': 'agent-assignment', 
    'project': 'bnb-agent-assignment',
    'zone': 'us-central1-a',
    'server_image': 'server-template',
    'client_image': 'client-template',
    'root_folder': '~/ExpoCloud',
    'project_folder': 'examples.agent_assignment'
}
engine = GCE(config)
```

The configuration is a dictionary with the following keys:

-   `prefix` - the prefix used for the automatically generated names of
    compute instances. Several experiments with different prefixes may
    be conducted simultaneously.

-   `project` - the name identifying the project on the cloud platform.

-   `zone` - the zone to which the allocated compute instances will
    pertain. The current implementation of the GCE engine is limited to
    use one zone. The limitation may be lifted in the future to enable
    an even larger scalability.

-   `server_image` and `client_image` - the names of the machine images
    storing configuration (such as the CPU family, the number of CPUs,
    the amount of RAM, etc) of all future server and client instances,
    respectively. An inexpensive configuration with one or two CPUs may
    be used for a server, while one may opt for 64 or 128 CPUs per
    instance for a client. We will see in the following sections that an
    ExpoCloud’s client makes use of all the available CPUs.

-   `root_folder` - the folder in which ExpoCloud resides. This folder
    should be the same both on the initial server instance and in the
    machine images.

-   `project_folder` - the folder in which the `run_server` and
    `run_client` scripts reside. The folder must be specified in the
    dotted format as shown in the listing. This is the format in which the path must be specified when using
the `-m` command-line argument to `python`.

In our case, the engine being used is Google Compute Engine (GCE). Some
dictionary keys for other engines may differ. For example, `zone` is a
GCE concept and a more suitable key name may be used in the extension
class for another platform. In the same vein, `LocalEngine`, which is
the engine class for running a local simulation of the experiment, is
configured with a single value, namely the project folder.

Lastly, all we have left to do is construct the server object and call
its `run` method:

```sh
Server(tasks, engine).run()
```

## The `run_client` script
`run_client` is a very short script, which imports the user-defined `Task` class and
runs the client object. For our experiment, it may look as follows:

```python
from src.client import Client
from examples.agent_assignment.task import Task
Client().run()
```

# Enjoy!