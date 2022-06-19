class Constants:
    """
    The constants used throughout the framework. All times are in seconds.
    """
    CLIENT_MAX_NON_ACTIVE_TIME = 300 # If no handshake, kill the instance
    SSH_RETRY_DELAY = 5 # If ssh fails, try again after this delay

    SERVER_CYCLE_WAIT = 0.1
    CLIENT_CYCLE_WAIT = 0.1
    CLIENT_WAIT_AFTER_SENDING_BYE = 5
    WORKER_WAIT_AFTER_DONE = 1
    SERVER_PORT = 8000 # Port for the queues managed by the server
    CLIENT_PORT = 3000 # Port for the queues managed by the client