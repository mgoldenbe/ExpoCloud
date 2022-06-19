import subprocess
import time

from src.util import handle_exception, my_ip

class InstanceType:
    CLIENT = 'client'
    SERVER = 'server'

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
        self.last_creation_timestamp = 0 # last created instance

    def can_create_instance(self):
        return \
            time.time() - self.last_creation_timestamp >= \
            self.creation_frequency_limit()

    def run_instance(self, type):
        """
        Create and run the instance of the given type.
        """
        try:
            result = self.create_instance_(type)
            if not result: return None
            _, ip = result
            command = {
                InstanceType.SERVER: 
                    f"{self.project_folder}.run_server",
                InstanceType.CLIENT: 
                    f"{self.project_folder}.run_client {my_ip()}"
            }[type]
            print(result, flush=True)
            print(command, flush=True)
            self.run_instance_(ip, command)
            return result
        except Exception as e:
            handle_exception(e, "Exception in the abstract engine's run_instance method", False)
            return None

    # Private methods 
    def create_instance_(self, type):
        """
        Create the instance of the given type.
        """
        name, image = {
            InstanceType.SERVER: 
                (f"{self.prefix}-server", self.server_image),
            InstanceType.CLIENT: 
                (f"{self.prefix}-client", self.client_image)
        }[type]
        result = \
            self.create_instance(name, image)
        if result:
            self.last_creation_timestamp = time.time()
        return result
    
    def run_instance_(self, ip, python_arg):
        key = '~/.ssh/id_rsa'
        command = f"cd {self.root_folder}; python -m {python_arg} >out 2>err &"
        ssh_command = \
            f"ssh {ip} -i {key} -o StrictHostKeyChecking=no \"{command}\""
        print(ssh_command, flush=True)
        status = subprocess.check_output(ssh_command, shell=True)
        print(f"Result of ssh: {status}", flush=True)