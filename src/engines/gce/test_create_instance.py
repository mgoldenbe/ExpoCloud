# Adapted from https://cloud.google.com/compute/docs/reference/rest/beta/instances/insert

from pprint import pprint

from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()

service = discovery.build('compute', 'beta', credentials=credentials)

project = 'iucc-novel-heuristic'
zone = 'us-central1-a'
instance = 'auto-instance'

instance_body = {
    "name": "from-image",
    "sourceMachineImage": "projects/iucc-novel-heuristic/global/machineImages/e-machine",
}

request = service.instances().insert(project=project, zone=zone, body=instance_body)
response = request.execute()

pprint(response)