# Adapted from https://cloud.google.com/compute/docs/reference/rest/beta/instances/insert

from pprint import pprint
from sys import stderr

from src.abstract_engine import AbstractEngine
from src.util import handle_exception

try:
    from googleapiclient import discovery
    from oauth2client.client import GoogleCredentials
except:
    print('It looks like you are not on GCE', file=stderr, flush=True)
    exit(1)
import time

class GCE(AbstractEngine):
    """
    The google compute engine.
    """
    def __init__(self, config):
        super().__init__(config)

    def creation_frequency_limit(self):
        """
        GCE does not allow more than one instance creation based on a machine image in 10 minutes.
        """
        return 600

    def create_instance_raw(self, name, image):
        """
        Creates a new instance based on the image with the given name. If successful, returns the name and internal ip of the new instance, which is formed from the `prefix` and the current timestamp. Otherwise, returns `None`.
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

    # https://cloud.google.com/compute/docs/reference/rest/beta/instances/delete
    def kill_instance(self, name):
        """
        Kills the specified instance.
        """
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
    def ip_from_name_(self, name):
        """
        Return the instance's internal IP based on it's name.
        This method should not be invoked directly. Rather, the users of this class should invoke the `create_instance` method.
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
                    print("It looks like this creation attempt was too early", 
                          file = stderr, flush=True)
                    return None
                time.sleep(5)
            return response['networkInterfaces'][0]['networkIP']
        except Exception as e:
            # should not happen
            handle_exception(e, "Could not get ip of instance", True) 
            return None

