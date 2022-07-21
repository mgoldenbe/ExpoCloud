import subprocess
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
    
    # For compatibility
    def next_instance_name(self, type):
        return next_instance_name(type, "")

    # For compatibility
    def create_instance(self, _name, _role):
        return util.my_ip()

    def run_instance(self, _ip, role, server_port):
        if role != InstanceRole.CLIENT: return None
        try:
            name, ip = 'localhost', util.my_ip()
            self.run_instance_(
                f"{self.project_folder}.run_client \"{util.my_ip()}\" {server_port}")
            return name, ip
        except Exception as e:
            util.handle_exception(e, f"Exception running new client", False)
            return None
    
    def kill_instance(self, name_):
        pass

    def run_instance_(self, python_arg):
        command = f"cd {self.root_folder}; python -m {python_arg} >out 2>err &"
        subprocess.check_output(command, shell=True)