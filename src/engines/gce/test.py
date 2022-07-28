# Adapted from https://cloud.google.com/compute/docs/reference/rest/beta/instances/insert

from dis import Instruction

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import time
import subprocess

def list_instances(project, zone):
    """
    Rurns list of tuples (name, ip, status) for each instance in the zone.
    """
    result = []

    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    request = service.instances().list(project=project, zone=zone)

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

result = list_instances(project = 'iucc-novel-heuristic', zone = 'us-central1-a')
print(result)
