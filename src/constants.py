import os
class Constants:
    """
    The constants used throughout the framework. All times are in seconds.
    """
    MIN_CREATION_DELAY = 30 # Initial delay for instance creation
    INSTANCE_MAX_NON_ACTIVE_TIME = 300 # If no handshake, kill the instance
    HEALTH_UPDATE_FREQUENCY = 10 # Frequency of health updates
    HEALTH_UPDATE_LIMIT = 300 # If no health update, kill the instance
    SSH_RETRY_DELAY = 5 # If ssh fails, try again after this delay
    CLIENTS_STOP_TIME = 5 # Time duration clients are kept stopped on server failure before the backup server is created
    
    OUTPUT_FOLDER = 'output'
    PICKLED_SERVER_FILE = os.path.join(OUTPUT_FOLDER, 'pickled')

    SERVER_CYCLE_WAIT = 0.1
    CLIENT_CYCLE_WAIT = 0.1
    CLIENT_WAIT_AFTER_SENDING_BYE = 5
    WORKER_WAIT_AFTER_DONE = 1