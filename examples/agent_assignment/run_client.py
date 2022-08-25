"""
This is the user-supplied script to create the :any:`Client` object and run it. The experiment-specific part of this script is the import of the :any:`Task` class.
"""

__author__ = "Meir Goldenberg"
__copyright__ = "Copyright 2022, The ExpoCloud Project"
__license__ = "MIT"
__version__ = "1.0"
__email__ = "mgoldenb@g.jct.ac.il"

from src.client import Client
from src.util import my_print
from src.constants import Verbosity
from examples.agent_assignment.task import Task

if __name__ == '__main__':
    my_print(Verbosity.all, "Running client")
    Client().run()