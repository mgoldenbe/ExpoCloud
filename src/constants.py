"""
The classes for controlling verbosity and for defining constants to be used throughout.
"""

__author__ = "Meir Goldenberg"
__copyright__ = "Copyright 2022, The ExpoCloud Project"
__license__ = "MIT"
__version__ = "1.0"
__email__ = "mgoldenb@g.jct.ac.il"

import os

class Verbosity:
    """
    Constants determining which output is to be printed.
    """

    all = True
    """
    Output not about a specific topic specified by the other constants.
    """

    instance_creation_etc = True
    """
    Output detailing the creation of cloud instances.
    """    

    workers = False
    """
    Output detailing the operation of the workers at a client.
    """

    message_sync = True
    """
    Output detailing the synchronization of messages from the primary and the backup servers to the clients.
    """ 

    messages = False
    """
    Output detailing the processing of messages.
    """ 

    all_non_health_messages = False
    """
    Output all non-health-report messages received by one of the servers.
    """ 

    failure_traceback = False
    """
    Output of the traceback record for failure events.
    """

    command_lines = True
    """
    Output of the command lines to be executed.
    """    


class Constants:
    """
    The constants used throughout the framework. All times are in seconds.
    """

    MIN_CREATION_DELAY = 30
    """
    The initial delay for instance creation.
    """

    INSTANCE_MAX_NON_ACTIVE_TIME = 300
    """
    If no handshake for this amount of time, kill the instance.
    """

    HEALTH_UPDATE_FREQUENCY = 1
    """
    Frequency of health updates
    """

    HEALTH_UPDATE_LIMIT = 60
    """
    An otherwise active instance that has not reported on its health for this amount of time is considered unhealthy.
    """

    SSH_RETRY_DELAY = 5
    """
    If ssh fails, attempt again after this delay.
    """

    CLIENTS_TIME_TO_STOP = 5
    """
    Time for clients so surely stop sending messages following the sending of the ``STOP`` event.
    """
    
    OUTPUT_FOLDER = 'output'
    """
    The folder for storing the output files.
    """

    PICKLED_SERVER_FILE = os.path.join(OUTPUT_FOLDER, 'pickled')
    """
    The file storing the serialized server object.
    """    

    SERVER_CYCLE_WAIT = 0.1
    """
    The delay between two successive iterations of the main server loop.
    """

    CLIENT_CYCLE_WAIT = 0.1
    """
    The delay between two successive iterations of the main client loop.
    """

    CLIENT_WAIT_AFTER_SENDING_BYE = 5
    """
    The delay between a client sending the ``BYE`` message to the server and completing.
    """

    WORKER_WAIT_AFTER_DONE = 1
    """
    The delay between a worker sending reporting the result to the client and completing.
    """