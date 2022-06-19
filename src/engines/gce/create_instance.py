# Adapted from https://cloud.google.com/compute/docs/reference/rest/beta/instances/insert

from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import time
import subprocess

def create_instance(prefix, image, project, zone):
    """
    Creates a new instance on the cloud and returns it's name, which is formed from the `prefix` and the current timestamp
    `image` - the name of the machine image
    `project` - the name of the project on the cloud
    `zone` - the name of the zone on the cloud
    """
    credentials = GoogleCredentials.get_application_default()

    service = discovery.build('compute', 'beta', credentials=credentials)

    instance_name = f"{prefix}-{round(time.time())}"

    instance_body = {
        "name": instance_name,
        "sourceMachineImage": 
            f"projects/{project}/global/machineImages/{image}",
    }

    request = service.instances().insert(project=project, zone=zone, body=instance_body)
    try:
        request.execute()
        return instance_name
    except:
        return None

# Adapted from https://stackoverflow.com/a/39096719/2725810
def ip_from_name(project, zone, name):
    """
    Return the instance's internal IP based on it's project, zone and name.
    """
    credentials = GoogleCredentials.get_application_default()
    api = discovery.build('compute', 'v1', credentials=credentials)

    request = api.instances().get(project=project, zone=zone, instance=name)
    try:
        response = request.execute()
        return response['networkInterfaces'][0]['networkIP']
    except:
        return None

ip = ip_from_name(
    project = 'iucc-novel-heuristic', 
    zone = 'us-central1-a',
    name = 'test-01')
key = '~/.ssh/id_rsa'
command = "cd mytemp; python test.py >out 2>err &"
result = subprocess.check_output(
    f"ssh {ip} -i {key} -o StrictHostKeyChecking=no \"{command}\"", shell=True)
print(result)
exit(1)
name = create_instance(
    prefix = 'client', 
    image = 'e-machine', 
    project = 'iucc-novel-heuristic', 
    zone = 'us-central1-a')
