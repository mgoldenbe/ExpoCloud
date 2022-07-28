from src.client import Client
from src.util import myprint
from src.constants import Verbosity
from examples.agent_assignment.task import Task

myprint(Verbosity.all, "Running client")
Client().run()