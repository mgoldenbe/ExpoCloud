import subprocess
import psutil
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
        self.name_to_pid = {}
    
    def is_local(self): return True

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
        args = ['python', '-m',
            {
                InstanceRole.CLIENT: f"{self.project_folder}.run_client",
                InstanceRole.BACKUP_SERVER: 'src.run_backup'
            }[role], 
            util.my_ip(), str(server_port), name
        ]
        if role == InstanceRole.CLIENT: args.append(str(max_cpus))

        with open(f"out-{name}", 'a') as out, open(f"err-{name}", 'a') as err:
            pid = \
                subprocess.Popen(args, stdout=out, stderr=err, shell=False).pid
            print(f"Created process {pid}")
            self.name_to_pid[name] = pid

    def kill_instance(self, name):
        # Process tree with full commands: ps auxfww
        time.sleep(2) # Give it time to shut shown
        pid = self.name_to_pid[name]
        print(f"Terminating process {pid}")
        try:
            proc = psutil.Process(pid)
        except:
            print("It's dead already", flush=True)
            return
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f"Process {pid} did not terminate in time, killing it")
            proc.kill()
