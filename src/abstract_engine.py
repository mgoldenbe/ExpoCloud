from ast import Constant
from ctypes import util
import time
import sys

from src.util import myprint
from src.constants import Verbosity
from src.util import handle_exception, my_ip, remote_execute, InstanceRole, next_instance_name
from src.constants import Constants

class AbstractEngine:
    """
    Parent for engines other than the local machine. We assume that the engine supports machine images. The children should implement two methods: `create_instance` returning the name and the internal IP address of the new instance, `kill_instance` killing the instance with the given name and `creation_frequency_limit` returning the number of seconds that needs to pass between two instance creations.
    """
    def __init__(self, config):
        """
        `config` should have the following keys:

        `prefix` - the prefix used for the names of instances
        `project` - the name of the project on the cloud
        `zone` - the name of the zone on the cloud
        `server_image` - the name of the machine image for a server
        `client_image` - the name of the machine image for a server
        `root_folder` - the path to the root folder, e.g. ~/ExpoCloud/
        `project_folder` - dotted path to run_server.py and run_client.py.
        """
        self.prefix = config['prefix']
        self.project = config['project']
        self.zone = config['zone']
        self.server_image = config['server_image']
        self.client_image = config['client_image']
        self.root_folder = config['root_folder']
        self.project_folder = config['project_folder']
        self.last_creation_timestamp = 0
        self.instance_id = {} # role->id, used to compute next instance name

        # No more instances till this passes;
        # halved, because doubling after the first failure.
        self.creation_delay = Constants.MIN_CREATION_DELAY / 2

    def is_local(self): return False

    def creation_attempt_allowed(self):
        """
        Makes sure that instance creation attempts are performed with exponentially increasing delays.
        """
        return \
            time.time() - self.last_creation_timestamp >= self.creation_delay

    def next_instance_name(self, type):
        return next_instance_name(type, self.prefix, self.instance_id)

    def image_name(self, type):
        if type == InstanceRole.CLIENT: return self.client_image
        return self.server_image

    def create_instance(self, name, type):
        """
        Create the instance of the given type.
        """
        if not self.creation_attempt_allowed(): return None
        myprint(Verbosity.instance_creation_etc, 
                f"Attempting to create {type} named {name}")
        ip = self.create_instance_raw(name, self.image_name(type))
        if not ip:
            self.creation_delay *= 2
            myprint(Verbosity.instance_creation_etc,
                    f"Next creation attempt in {self.creation_delay} seconds")
            return None
            
        myprint(Verbosity.all, f"New {name}")
        self.last_creation_timestamp = time.time()
        return ip

    def run_instance(self, name, ip, type, server_port, max_cpus = None):
        """
        Create and run the instance of the given type.
        """
        try:
            python_arg = {
                InstanceRole.BACKUP_SERVER: 
                    f"src.run_backup {my_ip()} {server_port} {name}",
                InstanceRole.CLIENT: 
                    f"{self.project_folder}.run_client {my_ip()} {server_port} {name} {max_cpus}"
            }[type]
            command = f"cd {self.root_folder}; python -m {python_arg} >out 2>err &"
            remote_execute(ip, command)
            self.creation_delay = Constants.MIN_CREATION_DELAY
        except Exception as e:
            handle_exception(e, "Exception in the abstract engine's run_instance method", True) # Stop, should never happen