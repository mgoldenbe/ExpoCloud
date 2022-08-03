from src.client import Client
from src.util import myprint
from src.constants import Verbosity
from examples.agent_assignment.task import Task

if __name__ == '__main__':
    myprint(Verbosity.all, "Running client")
    Client().run()