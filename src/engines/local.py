import subprocess
from sys import stderr
from src import util

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

    def can_create_instance(self):
        """
        Needed for compatibility.
        """
        return True

    def run_server(self):
        try:
            name, ip = 'localhost', util.my_ip()
            self.run_instance_(f"{self.project_folder}.run_server")
            return name, ip
        except:
            return None, None
    
    def run_client(self):
        try:
            name, ip = 'localhost', util.my_ip()
            self.run_instance_(
                f"{self.project_folder}.run_client \"{util.my_ip()}\"")
            return name, ip
        except Exception as e:
            print(f"Exception running client\n{str(e)}", 
                  file=stderr, flush=True)
            return None, None
    
    def kill_instance(self, name_):
        pass

    def run_instance_(self, python_arg):
        command = f"cd {self.root_folder}; python -m {python_arg} >out 2>err &"
        subprocess.check_output(command, shell=True)