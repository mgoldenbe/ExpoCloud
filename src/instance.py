# @ Meir Goldenberg The module is part of the ExpoCloud Framework

import time
import os
import sys
from src import util
from src.util import InstanceRole, handle_exception
from src.constants import Constants

def is_primary(instance):
    return instance.role == InstanceRole.PRIMARY_SERVER

def is_backup(instance):
    return instance.role == InstanceRole.BACKUP_SERVER

def is_client(instance):
    return instance.role == InstanceRole.CLIENT

class Instance():
    def __init__(self, role, engine):
        """
        Objects of this class represent instances.
        `role` - either InstanceRole.CLIENT or InstanceRole.BACKUP_SERVER.
        `engine` - an instance of either AbstractEngine's subclass or LocalEngine.
        """
        self.role = role
        self.engine = engine
        self.active_timestamp = None # last health update, None until handshake
        self.name = None
        if engine:
            self.name = engine.next_instance_name(role)
        self.ip = None
    
    def create(self):
        """
        Create the instance
        """
        self.ip = self.engine.create_instance(self.name, self.role)
        if self.ip: self.creation_timestamp = time.time()

    def run(self, server_port, max_cpus = None):
        self.engine.run_instance(self.name, self.ip, self.role, server_port, max_cpus)
    
    def __del__(self):
        if self.ip and self.engine:
            print(f"The {self.role} {self.name} is being killed", 
                file=sys.stderr, flush=True)
            self.engine.kill_instance(self.name)
    
    def is_healthy(self, tasks_remain: bool):
        """
        `task remain` - a Boolean indicating whether there are still tasks remaining.
        Returns true if the instance is healthy, i.e. either:
        - The instance has no IP address (so that no real instance had been created) and there are task remaining.
        - The instance has an IP address, but is not active (i.e. has not shaken hands with the primary server). For client instance, we also require that there still be tasks remaining as indicated. For all types of instances, INSTANCE_MAX_NON_ACTIVE_TIME has not passed since its creation.
        - It is active and HEALTH_UPDATE_LIMIT has not passed since last health 
          update.
        """
        if (not self.ip) and tasks_remain: return True

        if self.ip and (not self.active_timestamp) and \
           (tasks_remain or is_backup(self)):
            if time.time() - self.creation_timestamp <= \
               Constants.INSTANCE_MAX_NON_ACTIVE_TIME: return True

        if self.active_timestamp:
            if time.time() - self.active_timestamp <= \
               Constants.HEALTH_UPDATE_LIMIT: return True

        if self.active_timestamp or (is_client(self) and tasks_remain):
            print(f"{self.name} is unhealthy", file=sys.stderr, flush=True)
            print(f"Created {self.creation_timestamp}",
                  f"last healthy {self.active_timestamp}", 
                  file=sys.stderr, flush=True)
        else:
            print(f"Inactive {self.name}", 
                  file=sys.stderr, flush=True)
        return False

    def shake_hands(self):
        print(f"{self.name} shook hands", flush=True)
        self.active_timestamp = time.time()

class ClientInstance(Instance):
    def __init__(self, engine, tasks_from_failed):
        """
        Objects of this class represent clients. 
        `engine` - an instance of either AbstractEngine's subclass or LocalEngine.
        `tasks_from_failed` - the list to which the tasks assigned to this client are to be appended should this client fail.
        """
        super().__init__(InstanceRole.CLIENT, engine)
        self.tasks_from_failed = tasks_from_failed
        self.my_tasks = [] # ids of tasks held by the client

        # The following is for backup server - ids of client messages forwarded by primary server not yet matched by direct messages from the client
        self.received_ids = []
    
    def __del__(self):
        if self.active_timestamp:
            self.events_file.close()
            self.exceptions_file.close()
        self.tasks_from_failed += self.my_tasks
        super().__del__()
    
    def connect(self, server_role: str):
        """
        Connect to the appropriate queues
        """
        self.inbound_q, self.outbound_q = None, None
        try:
            if server_role == InstanceRole.PRIMARY_SERVER:
                self.inbound_q, self.outbound_q = \
                    util.get_guest_qs(
                        self.ip, self.port, ['to_primary_q', 'from_primary_q'])
            else:
                assert(server_role == InstanceRole.BACKUP_SERVER)
                self.inbound_q, self.outbound_q = \
                    util.get_guest_qs(
                        self.ip, self.port, ['to_backup_q', 'from_backup_q'])
        except:
            pass # if the client has died, it will be handled elsewhere

    def init_files(self, parent_dir):
        path = os.path.join(parent_dir, self.name)
        os.makedirs(path, exist_ok=True)
        self.events_file = open(os.path.join(path, 'events.txt'), "a")
        self.exceptions_file = open(os.path.join(path, 'exceptions.txt'), "a")
        
    def shake_hands(self, server_role, parent_dir: str):
        print(f"{server_role} attempting to connect to client queues", flush=True)
        self.connect(server_role)
        print(f"{server_role} connected to client queues", flush=True)
        self.init_files(parent_dir)
        super().shake_hands()

    def register_tasks(self, tasks):
        self.my_tasks += [t.id for t in tasks]
        for t in tasks:
            print(f"Task {t.id} is registered with {self.name}")
    
    def unregister_task(self, t_id):
        self.my_tasks = list(filter(lambda i: i != t_id, self.my_tasks))

    def unregister_domino(self, tasks, hardness):
        hard = [t_id for t_id in self.my_tasks 
                if tasks[t_id].hardness >= hardness]
        self.my_tasks = list(filter(lambda i: i not in hard, self.my_tasks))

class BackupServerInstance(Instance):
    """
    Objects of this class represent backup servers. 
    """
    def __init__(self, engine):
        """
        `engine` - an instance of either AbstractEngine's subclass or LocalEngine.
        """
        super().__init__(InstanceRole.BACKUP_SERVER, engine)

    def shake_hands(self):
        self.inbound_q, self.outbound_q = \
            util.get_guest_qs(
                self.ip, self.port, ['to_primary_q', 'from_primary_q'])
        super().shake_hands()

class PrimaryServerInstance:
    """
    In backup server, an object of this class represents the primary server. Note that this class does not inherit from `Instance`.
    """
    def __init__(self, my_port):
        self.role = InstanceRole.PRIMARY_SERVER
        self.ip = util.command_arg_ip()
        self.manager = util.make_manager(
            ['to_primary_q', 'from_primary_q'], my_port)
        self.outbound_q, self.inbound_q = \
            self.manager.to_primary_q(), self.manager.from_primary_q()
        self.active_timestamp = time.time()
    
    def is_healthy(self):
        return time.time() - self.active_timestamp <= \
               Constants.HEALTH_UPDATE_LIMIT