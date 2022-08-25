"""
The class for working with the Google Compute Engine.
"""

__author__ = "Meir Goldenberg"
__copyright__ = "Copyright 2022, The ExpoCloud Project"
__license__ = "MIT"
__version__ = "1.0"
__email__ = "mgoldenb@g.jct.ac.il"

from sys import stderr
from typing import Union, List

from src.util import my_print
from src.constants import Verbosity

from src import util
from src.util import InstanceRole, next_instance_name
from src.abstract_engine import AbstractEngine
from src.util import handle_exception

try:
    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials
except:
    if __name__ == '__main__':
        my_print(Verbosity.all, 'It looks like you are not on GCE')
        exit(1)
import time

class GCE(AbstractEngine):
    """
    The google compute engine.
    """
    def __init__(self, config):
        """
        The constructor.

        :param config: The configuration dictionary with the following keys:

        * ``prefix`` - the prefix used for the names of instances.
        * ``project`` - the name of the project on the cloud.
        * ``zone`` - the zone where the instances are to be located.
        * ``server_image`` - the name of the machine image for a server.
        * ``client_image`` - the name of the machine image for a server.
        * ``root_folder`` - the path to the root folder, e.g. ~/ExpoCloud/.
        * ``project_folder`` - the path to the experiment in dot notation, e.g. ``'examples.agent_assignment'``.

        :type config: dict
        """
        self.zone = config['zone']
        super().__init__(config)

    # Adapted from https://cloud.google.com/compute/docs/reference/rest/beta/instances/insert
    def create_instance_native(self, name: str, image: str) -> Union[str, None]:
        """
        Creates a new instance based on the image with the given name. If successful, returns the internal IP address of the new instance. Otherwise, returns ``None``.

        :param name: The name to be assigned to the new instance.
        :type name: str
        :param image: The name of the machine image for the new instance.
        :type image: str
        :return: The internal IP address of the new instance or ``None``.
        :rtype: Union[str, None]
        """        
        credentials = GoogleCredentials.get_application_default()

        service = discovery.build('compute', 'beta', credentials=credentials)

        instance_body = {
            "name": name,
            "sourceMachineImage": 
                f"projects/{self.project}/global/machineImages/{image}",
        }

        request = service.instances().insert(
            project=self.project, zone=self.zone, body=instance_body)
        try:
            request.execute()
        except Exception as e:
            handle_exception(e, "Could not create instance", False)
            return None
        ip = self.ip_from_name_(name)
        if not ip:
            self.kill_instance(name)
            return None
        return ip

    def list_instances(self) -> List[tuple]:
        """
        Returns list of tuples ``(<name>, <ip>, <status>)`` for each instance.
        :return: The list of tuples ``(<name>, <ip>, <status>)`` for each instance.
        :rtype: List[tuple]
        """
        result = []

        credentials = GoogleCredentials.get_application_default()
        service = discovery.build('compute', 'v1', credentials=credentials)
        request = service.instances().list(project=self.project, zone=self.zone)

        while request is not None:
            response = request.execute()
            for instance in response['items']:
                result.append(
                    (instance['name'], 
                    instance['networkInterfaces'][0]['networkIP'],
                    instance['status']))

            request = service.instances().list_next(
                previous_request=request, previous_response=response)

        return result
        
    # https://cloud.google.com/compute/docs/reference/rest/beta/instances/delete
    def kill_instance(self, name) -> str:
        """
        Terminates the specified instance.

        :param name: The name of the instance to terminate.
        :type name: str
        :return: The name of the terminated instance or ``None`` in the case of an exception.
        :rtype: str
        """
        #myprint(Verbosity.all, "Instance killing is disabled")
        # return
        credentials = GoogleCredentials.get_application_default()
        service = discovery.build('compute', 'beta', credentials=credentials)

        request = service.instances().delete(
            project=self.project, zone=self.zone, instance=name)
        try:
            request.execute()
            return name
        except Exception as e:
            handle_exception(e, "Could not kill instance", False)
            return None
    
    # Adapted from https://stackoverflow.com/a/39096719/2725810
    def ip_from_name_(self, name: str) -> Union[str, None]:
        """
        Return the IP address of the instance with the specified name. The internal IP address is returned. This method should not be invoked directly.

        :param name: The name of the instance whose IP address is required.
        :type name: str
        :return: The IP address of the instance with the specified name or ``None`` in the case of an exception.
        :rtype: Union[str, None]
        """
        credentials = GoogleCredentials.get_application_default()
        api = discovery.build('compute', 'v1', credentials=credentials)

        request = api.instances().get(
            project=self.project, zone=self.zone, instance=name)
        try:
            while True: # wait till the instance is created
                response = request.execute()
                if response['status'] == 'RUNNING': break
                if response['status'] == 'STOPPING':
                    my_print(Verbosity.instance_creation_etc, 
                            f"The attempt to create {name} was too early")
                    return None
                time.sleep(5)
            return response['networkInterfaces'][0]['networkIP']
        except Exception as e:
            # should not happen
            handle_exception(e, "Could not get ip of instance", True) 
            return None

