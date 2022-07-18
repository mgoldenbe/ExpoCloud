import subprocess
from sys import stderr
from src import util
from src.util import InstanceRole

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
        self.client_running = False

    def creation_attempt_allowed(self):
        """
        Return True only if the client has not been run.
        """
        return not self.client_running
    
    # For compatibility
    def next_instance_name(self, type):
        return None

    # For compatibility
    def create_instance(self, _name, _role):
        if self.creation_attempt_allowed(): return util.my_ip()
        return None

    def run_instance(self, _ip, role):
        if role != InstanceRole.CLIENT: return None
        try:
            name, ip = 'localhost', util.my_ip()
            self.run_instance_(
                f"{self.project_folder}.run_client \"{util.my_ip()}\"")
            self.client_running = True
            return name, ip
        except Exception as e:
            util.handle_exception(e, f"Exception running new client", False)
            return None
    
    def kill_instance(self, name_):
        pass

    def run_instance_(self, python_arg):
        command = f"cd {self.root_folder}; python -m {python_arg} >out 2>err &"
        print(command, flush=True)
        subprocess.check_output(command, shell=True)