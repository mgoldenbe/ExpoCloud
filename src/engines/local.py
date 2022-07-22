import subprocess
import time
from sys import stderr
from src import util
from src.util import InstanceRole, next_instance_name

class LocalEngine:
    """
    The engine representing the local machine.
    """
    def __init__(self, project_folder):
        """
        project_folder should be in dot notation, e.g. 'examples.asp'
        """
        self.root_folder = util.get_project_root()
        self.project_folder = project_folder
        self.last_instance_timestamp = 0
        self.time_between_instances = 10
    
    # For compatibility
    def next_instance_name(self, type):
        return next_instance_name(type, "")

    # For compatibility
    def create_instance(self, _name, _role):
        if time.time() - self.last_instance_timestamp <= \
           self.time_between_instances: 
           return None
        self.last_instance_timestamp = time.time()
        return util.my_ip()

    def run_instance(self, name, _ip, role, server_port, max_cpus = None):
        if role != InstanceRole.CLIENT: return None
        try:
            self.run_instance_(
                f"{self.project_folder}.run_client \"{util.my_ip()}\" {server_port} {name} {max_cpus}")
        except Exception as e:
            util.handle_exception(e, f"Exception running new client", False)
    
    def kill_instance(self, name_):
        pass

    def run_instance_(self, python_arg):
        command = f"cd {self.root_folder}; python -m {python_arg} >out 2>err &"
        subprocess.check_output(command, shell=True)