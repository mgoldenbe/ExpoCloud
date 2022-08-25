"""
The parent class for all cloud-based compute engines.
"""

__author__ = "Meir Goldenberg"
__copyright__ = "Copyright 2022, The ExpoCloud Project"
__license__ = "MIT"
__version__ = "1.0"
__email__ = "mgoldenb@g.jct.ac.il"

from ast import Constant
import time
import sys
from typing import List

from src.util import my_print, extended_prefix
from src.constants import Verbosity
from src.util import handle_exception, my_ip, remote_execute, InstanceRole, next_instance_name
from src.constants import Constants

class AbstractEngine:
    """
    The parent class for all cloud-based compute engines. We assume that the engine supports machine images. The children should implement two methods: ``create_instance_native`` for creating the compute instance and ``kill_instance`` for terminating a compute instance.
    """
    def __init__(self, config: dict):
        """
        The constructor.

        :param config: The configuration dictionary with the following keys:

        * ``prefix`` - the prefix used for the names of instances.
        * ``project`` - the name of the project on the cloud.
        * ``server_image`` - the name of the machine image for a server.
        * ``client_image`` - the name of the machine image for a server.
        * ``root_folder`` - the path to the root folder, e.g. ~/ExpoCloud/.
        * ``project_folder`` - the path to the experiment in dot notation, e.g. ``'examples.agent_assignment'``.

        :type config: dict
        """              
        self.prefix = config['prefix']
        self.project = config['project']
        self.server_image = config['server_image']
        self.client_image = config['client_image']
        self.root_folder = config['root_folder']
        self.project_folder = config['project_folder']
        self.last_creation_timestamp = 0
        self.instance_id = {} # role->id, used to compute next instance name

        # No more instances till this passes;
        # halved, because doubling after the first failure.
        self.creation_delay = Constants.MIN_CREATION_DELAY / 2

    def is_local(self) -> bool:
        """
        Returns ``False`` to signify that this engine is not local.

        :return: ``False``
        :rtype: bool
        """        
        return False

    def creation_attempt_allowed(self) -> bool:
        """
        Check whether ``self.creation_delay`` seconds have passed since the last attempt of instance creation.

        :return: ``True`` if ``self.creation_delay`` seconds have passed since the last attempt of instance creation and ``False`` otherwise.
        :rtype: bool
        """        
        return \
            time.time() - self.last_creation_timestamp >= self.creation_delay

    def next_instance_name(self, role: InstanceRole) -> str:
        """
        Generate the name for the next instance with the given role.

        :param role: The role of the instance.
        :type role: InstanceRole
        :return: The name for the next instance with the given role.
        :rtype: str
        """        
        return next_instance_name(role, self.prefix, self.instance_id)

    def image_name(self, role: InstanceRole) -> str:
        """
        Return the name of the machine image for an instance with the given role.

        :param role: The role of the instance.
        :type role: InstanceRole
        :return: The name of the machine image for an instance with the given role.
        :rtype: str
        """      
        if role == InstanceRole.CLIENT: return self.client_image
        return self.server_image

    def create_instance(self, name: str, role:InstanceRole) -> str:
        """
        Create the instance of the given type and return its IP address.

        :param name: The name of the instance to be created.
        :type name: str
        :param role: The role of the instance to be created.
        :type role: InstanceRole
        :return: The IP address of the newly created instance.
        :rtype: str
        """        
        if not self.creation_attempt_allowed(): return None
        my_print(Verbosity.instance_creation_etc, 
                f"Attempting to create {role} named {name}")
        ip = self.create_instance_native(name, self.image_name(role))
        if not ip:
            self.creation_delay *= 2
            my_print(Verbosity.instance_creation_etc,
                    f"Next creation attempt in {self.creation_delay} seconds")
            return None
            
        my_print(Verbosity.all, f"New {name}")
        self.last_creation_timestamp = time.time()
        return ip

    def run_instance(self, name: str, ip: str, role: InstanceRole, 
                     server_port: int, max_cpus:int = None):
        """
        Run the instance.

        :param name: The name of the instance to be run.
        :type name: str
        :param ip: The IP address of the instance to be run.
        :type ip: str
        :param role: The role of the instance to be run.
        :type role: InstanceRole
        :param server_port: The port for handshaking with the primary server.
        :type server_port: int
        :param max_cpus: The maximum number of workers to be used by the client instance, defaults to ``None``, i.e. unlimited.
        :type max_cpus: int, optional
        """        
        try:
            python_arg = {
                InstanceRole.BACKUP_SERVER: 
                    f"src.run_backup {my_ip()} {server_port} {name}",
                InstanceRole.CLIENT: 
                    f"{self.project_folder}.run_client {my_ip()} {server_port} {name} {max_cpus}"
            }[role]
            command = f"cd {self.root_folder}; python -m {python_arg} >out 2>err &"
            remote_execute(ip, command)
            self.creation_delay = Constants.MIN_CREATION_DELAY
        except Exception as e:
            handle_exception(e, "Exception in the abstract engine's run_instance method", True) # Stop, should never happen
    
    def list_instances(self) -> List[tuple]:
        """
        Returns list of tuples ``(<name>, <ip>, <status>)`` for each instance. The particular engine subclasses should implemented this method.

        :return: The list of tuples ``(<name>, <ip>, <status>)`` for each instance.
        :rtype: List[tuple]
        """
        return []

    def kill_instance(self, name: str) -> str:
        """
        Terminates the specified instance. The particular engine subclasses should implemented this method.

        :param name: The name of the instance to terminate.
        :type name: str
        :return: The name of the terminated instance or ``None`` in the case of an exception.
        :rtype: str
        """
        return None
    
    def kill_dangling_clients(self, existing_clients: List[str]):
        """
        Kill clients whose name has the given prefix, but is not in the provided list of names. The method relies on the particular engine implementing the `list_instances` method.

        :param prefix: The given prefix.
        :type prefix: str
        :param existing_clients: The names of instances that are not to be killed.
        :type existing_clients: List[str]
        """
        prefix = extended_prefix(InstanceRole.CLIENT, self.prefix)   
        for name in \
            [el[0] for el in self.list_instances() 
                   if el[0].startswith(prefix) and \
                      el[0] not in existing_clients]:
            self.kill_instance(name)
        
