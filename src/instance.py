"""
Classes for representing a cloud instance at either the primary or the backup server.
"""

import time
import os
import sys
from typing import List, Union
from src.abstract_engine import AbstractEngine
from src.abstract_task import AbstractTask
from src.engines.local import LocalEngine

from src.util import my_print
from src.constants import Verbosity

from src import util
from src.util import InstanceRole, handle_exception
from src.constants import Constants

class Instance():
    """
    The parent class for classes representing a cloud instance at either the primary or the backup server. This class is only for representing a backup server or a client instance. Note the class for representing a primary server in the backup server does not derive from this class.
    """    
    def __init__(self, 
                 role: InstanceRole, 
                 engine: Union[AbstractEngine, LocalEngine]):                 
        """
        The constructor.

        :param role: The role of the instance.
        :type role: InstanceRole
        :param engine: The engine being used.
        :type engine: Union[AbstractEngine, LocalEngine])
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
        Create the cloud instance.
        """
        self.ip = self.engine.create_instance(self.name, self.role)
        if self.ip: self.creation_timestamp = time.time()

    def run(self, server_port, max_cpus = None):
        """
        Run the instance.
        """
        self.engine.run_instance(self.name, self.ip, self.role, server_port, max_cpus)
    
    def __del__(self):
        """
        The destructor, terminates the cloud instance.
        """        
        if self.ip and self.engine:
            if not self.engine.is_local(): # for local, just let it complete
                self.engine.kill_instance(self.name)
    
    def is_healthy(self, tasks_remain: bool):
        """
        Returns true if the instance is healthy, i.e. either:

        * The instance has no IP address (so that no real instance had been created) and there are task remaining.
        * The instance has an IP address, but is not active (i.e. has not shaken hands with the primary server). For client instance, we also require that there still be tasks remaining as indicated. For all types of instances, INSTANCE_MAX_NON_ACTIVE_TIME has not passed since its creation.
        * It is active and HEALTH_UPDATE_LIMIT has not passed since last health 
          update.

        :param tasks_remain: ``True`` if there are tasks remaining and ``False`` otherwise.
        :type tasks_remain: bool
        :return: _description_
        :rtype: _type_
        """

        if self.active_timestamp:
            result = \
                time.time() - self.active_timestamp <= \
                Constants.HEALTH_UPDATE_LIMIT
            if not result: 
                my_print(Verbosity.all, 
                        f"{self.name} failed to send health updates for too long.   Last update: {util.short_timestamp(self.active_timestamp)}")
            return result
        
        assert(not self.active_timestamp)

        if self.ip and \
           time.time() - self.creation_timestamp > \
           Constants.INSTANCE_MAX_NON_ACTIVE_TIME:
            my_print(Verbosity.all, f"{self.name} failed to shake hands for too long.   Creation: {util.short_timestamp(self.creation_timestamp)}") 
            return False
        
        if is_backup(self): return True
        if is_client(self):
            result = tasks_remain
            if not result:
                my_print(Verbosity.all, f"{self.name} is unneeded")
            return result

        assert(False)

    def shake_hands(self):
        """
        Shake hands with the instance. The common action implemented here is storing the timestamp of when the instance became active, where being *active* means that the handshake with it has taken place.
        """        
        my_print(Verbosity.all, f"{self.name} shook hands")
        self.active_timestamp = time.time()

class ClientInstance(Instance):
    """
    The class for representing a client instance in either the primary or the backup server.
    """    
    def __init__(self, 
                 engine: Union[AbstractEngine, LocalEngine], 
                 tasks_from_failed: List[AbstractTask]):
        """
        The constructor.

        :param engine: The engine being used.
        :type engine: Union[AbstractEngine, LocalEngine]
        :param tasks_from_failed: The list in which to store the tasks assigned to the client in the case of failure.
        :type tasks_from_failed: List[AbstractTask]
        """        
        
        super().__init__(InstanceRole.CLIENT, engine)
        self.tasks_from_failed = tasks_from_failed
        self.my_tasks = [] # ids of tasks held by the client

        # The following is for backup server - ids of client messages forwarded by primary server not yet matched by direct messages from the client
        self.received_ids = []
    
    def __del__(self):
        """
        The destructor.
        """        
        if self.active_timestamp:
            self.events_file.close()
            self.exceptions_file.close()
        self.tasks_from_failed += self.my_tasks
        super().__del__()
    
    def connect(self, server_role: InstanceRole):
        """
        Connect to the appropriate queues of the server.

        :param server_role: The role of the server at which the client instance is being represented.
        :type server_role: InstanceRole
        """        
        
        self.inbound_q, self.outbound_q = None, None
        try:
            if server_role == InstanceRole.PRIMARY_SERVER:
                self.inbound_q, self.outbound_q = \
                    util.get_guest_qs(
                        self.ip, self.port_primary, 
                        ['outbound', 'inbound'])
            else:
                assert(server_role == InstanceRole.BACKUP_SERVER)
                self.inbound_q, self.outbound_q = \
                    util.get_guest_qs(
                        self.ip, self.port_backup, 
                        ['outbound', 'inbound'])
        except:
            pass # if the client has died, it will be handled elsewhere

    def init_files(self, parent_dir: str):
        """
        Open files for storing events and exceptions from this client.

        :param parent_dir: The folder to contain the files.
        :type parent_dir: str
        """        
        path = os.path.join(parent_dir, self.name)
        os.makedirs(path, exist_ok=True)
        self.events_file = open(os.path.join(path, 'events.txt'), "a")
        self.exceptions_file = open(os.path.join(path, 'exceptions.txt'), "a")
        
    def shake_hands(self, server_role: InstanceRole, parent_dir: str):
        """
        Shake hands with the client.

        :param server_role: The role of the server at which the client is being represented.
        :type server_role: InstanceRole
        :param parent_dir: The folder within which the folder with the files related to this client is to be contained.
        :type parent_dir: str
        """        
        my_print(Verbosity.instance_creation_etc, 
                f"{server_role} attempting to connect to client queues")
        self.connect(server_role)
        my_print(Verbosity.instance_creation_etc,
                f"{server_role} connected to client queues")
        self.init_files(parent_dir)
        super().shake_hands()

    def register_tasks(self, tasks: List[AbstractTask]):
        """
        Register tasks assigned to this client.

        :param tasks: The newly assigned tasks.
        :type tasks: List[AbstractTask]
        """        
        self.my_tasks += [t.id for t in tasks]
        for t in tasks:
            my_print(Verbosity.all, f"Task {t.id} is at {self.name}")
    
    def unregister_task(self, t_id: int):
        """
        Unregister a given task from this client.

        :param t_id: The task id.
        :type t_id: int
        """        
        self.my_tasks = list(filter(lambda i: i != t_id, self.my_tasks))

    def unregister_domino(self, tasks: List[AbstractTask], hardness: tuple):
        """
        Unregister all tasks that have been previously registered with this client and can now be proven to be hard due to a task with the given :paramref:`hardness` having timed out.

        :param tasks: All the tasks.
        :type tasks: List[AbstractTask]
        :param hardness: The hardness of the task that has timed out.
        :type hardness: tuple
        """        
        hard = [t_id for t_id in self.my_tasks 
                if tasks[t_id].hardness >= hardness]
        self.my_tasks = list(filter(lambda i: i not in hard, self.my_tasks))

class BackupServerInstance(Instance):
    """
    The class for representing the backup server instance in the primary server.
    """    
    def __init__(self, engine: Union[AbstractEngine, LocalEngine]):
        """
        The constructor.

        :param engine: The engine being used.
        :type engine: Union[AbstractEngine, LocalEngine]
        """
        super().__init__(InstanceRole.BACKUP_SERVER, engine)

    def shake_hands(self):
        """
        Shake hands with the backup server.
        """
        self.inbound_q, self.outbound_q = \
            util.get_guest_qs(
                self.ip, self.port, ['to_primary_q', 'from_primary_q'])
        super().shake_hands()

class PrimaryServerInstance:
    """
    The class for representing the primary server instance in the backup server.
    """ 
    def __init__(self, my_port:int):
        """
        The constructor.

        :param my_port: The port for shaking hands with the primary server.
        :type my_port: int
        """
        self.role = InstanceRole.PRIMARY_SERVER
        self.ip = util.command_arg_ip()
        self.name = 'primary-server'
        self.manager = util.make_manager(
            ['to_primary_q', 'from_primary_q'], my_port)
        self.outbound_q, self.inbound_q = \
            self.manager.to_primary_q(), self.manager.from_primary_q()
        self.active_timestamp = time.time()
    
    def is_healthy(self):
        """
        Returns true if the primary server is healthy, i.e. HEALTH_UPDATE_LIMIT has not passed since last health update.
        """        
        return time.time() - self.active_timestamp <= \
               Constants.HEALTH_UPDATE_LIMIT

def is_primary(instance: Instance) -> bool:
    """
    Determine whether the instance represented by :paramref:`instance` is the primary server.

    :param instance: The object representing the instance.
    :type instance: Instance
    :return: ``True`` if the instance represented by :paramref:`instance` is the primary server and ``False`` otherwise.
    :rtype: bool
    """    
    return instance.role == InstanceRole.PRIMARY_SERVER

def is_backup(instance: Instance) -> bool:
    """
    Determine whether the instance represented by :paramref:`instance` is the backup server.

    :param instance: The object representing the instance.
    :type instance: Instance
    :return: ``True`` if the instance represented by :paramref:`instance` is the backup server and ``False`` otherwise.
    :rtype: bool
    """
    return instance.role == InstanceRole.BACKUP_SERVER

def is_client(instance: Instance) -> bool:
    """
    Determine whether the instance represented by :paramref:`instance` is a client.

    :param instance: The object representing the instance.
    :type instance: Instance
    :return: ``True`` if the instance represented by :paramref:`instance` is a client and ``False`` otherwise.
    :rtype: bool
    """
    return instance.role == InstanceRole.CLIENT