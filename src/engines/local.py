"""
The class implementing the engine representing the local machine.
"""

import subprocess
import psutil
import time
from sys import stderr

from src.util import my_print
from src.constants import Verbosity

from src import util
from src.util import InstanceRole, next_instance_name

class LocalEngine:
    """
    The engine representing the local machine.
    """

    TIME_BETWEEN_INSTANCES = 10
    """
    The time in seconds that must elapse between creation of instances.
    """

    def __init__(self, project_folder: str):
        """
        The constructor.

        :param project_folder: The project folder in dot notation, e.g. ``'examples.agent_assignment'``.
        :type project_folder: str
        """        
        self.root_folder = util.get_project_root()
        self.project_folder = project_folder
        self.last_instance_timestamp = 0
        self.name_to_pid = {}
        self.instance_id = {} # role->id, used to compute next instance name
    
    def is_local(self):
        """
        Returns ``True`` to signify that this engine is local.

        :return: ``True``
        :rtype: bool
        """
        return True

    def next_instance_name(self, role: InstanceRole) -> str:
        """
        Generate the name for the next instance with the given role.

        :param role: The role of the instance.
        :type role: InstanceRole
        :return: The name for the next instance with the given role.
        :rtype: str
        """
        return next_instance_name(role, "", self.instance_id)

    # For compatibility
    def create_instance(self, _name, _role) -> str:
        """
        Create the instance of the given type and return its IP address. For the local engine, this simply returns the IP address of the local host.

        :return: The IP address of the local host.
        :rtype: str
        """
        if time.time() - self.last_instance_timestamp <= \
           self.TIME_BETWEEN_INSTANCES: 
           return None
        self.last_instance_timestamp = time.time()
        return util.my_ip()

    def run_instance(
        self, name: str, _ip, role: InstanceRole, 
        server_port: int, max_cpus: int = None):
        """
        Run the instance.

        :param name: The name of the instance to be run.
        :type name: str
        :param role: The role of the instance to be run.
        :type role: InstanceRole
        :param server_port: The port for handshaking with the primary server.
        :type server_port: int
        :param max_cpus: The maximum number of workers to be used by the client instance, defaults to ``None``, i.e. unlimited.
        :type max_cpus: int, optional
        """
        args = ['python', '-m',
            {
                InstanceRole.CLIENT: f"{self.project_folder}.run_client",
                InstanceRole.BACKUP_SERVER: 'src.run_backup'
            }[role], 
            util.my_ip(), str(server_port), name
        ]
        if role == InstanceRole.CLIENT: args.append(str(max_cpus))
        my_print(Verbosity.command_lines, f"Args for running instance:\n{args}")

        with open(f"out-{name}", 'a') as out, open(f"err-{name}", 'a') as err:
            pid = \
                subprocess.Popen(args, stdout=out, stderr=err, shell=False).pid
            my_print(Verbosity.all, f"Created process {pid}")
            self.name_to_pid[name] = pid

    def kill_instance(self, name: str):
        """
        Terminates the specified instance.

        :param name: The name of the instance to terminate.
        :type name: str
        """        
        # Process tree with full commands: ps auxfww
        time.sleep(2) # Give it time to shut shown
        pid = self.name_to_pid[name]
        my_print(Verbosity.all, f"Terminating process {pid}")
        try:
            proc = psutil.Process(pid)
        except:
            my_print(Verbosity.all, f"Process {pid} is dead already")
            return
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            my_print(Verbosity.all, 
                    f"Process {pid} did not terminate in time, killing it")
            proc.kill()
    
    def kill_dangling_instances(self, _existing_clients):
        """
        This method is provided for the local engine for compatibility only.
        """
        pass