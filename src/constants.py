import os

class Verbosity:
    all = True
    instance_creation_etc = True

    workers = False
    message_sync = True
    messages = True
    all_non_health_messages = False
    failure_traceback = False
    command_lines = True


class Constants:
    """
    The constants used throughout the framework. All times are in seconds.
    """
    MIN_CREATION_DELAY = 30 # Initial delay for instance creation
    INSTANCE_MAX_NON_ACTIVE_TIME = 300 # If no handshake, kill the instance
    HEALTH_UPDATE_FREQUENCY = 1 # Frequency of health updates
    HEALTH_UPDATE_LIMIT = 10 # If no health update, kill the instance
    SSH_RETRY_DELAY = 5 # If ssh fails, try again after this delay
    CLIENTS_TIME_TO_STOP = 5 # Time for clients so surely stop sending messages
    
    OUTPUT_FOLDER = 'output'
    PICKLED_SERVER_FILE = os.path.join(OUTPUT_FOLDER, 'pickled')

    SERVER_CYCLE_WAIT = 0.1
    CLIENT_CYCLE_WAIT = 0.1
    CLIENT_WAIT_AFTER_SENDING_BYE = 5
    WORKER_WAIT_AFTER_DONE = 1