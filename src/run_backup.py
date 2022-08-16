"""
The script for running the backup server on the backup server instance, which consists of:

* Deserializing the primary server object.
* Transforming this object to represent the backup server by invoking the :any:`assume_backup_role<src.server.Server.assume_backup_role>` method.
* Running the server by invoking the :any:`run<src.server.Server.run>` method.

Note that this script is provided by the framework and is not to be supplied by the user.
"""

from src.constants import Constants
import multiprocessing
import pickle

from src.util import my_print
from src.constants import Verbosity

from src import util
import time

if __name__ == '__main__':
    my_print(Verbosity.all, "Running backup server")
    path = util.output_folder(util.command_arg_name())
    with open(util.pickled_file_name(path), 'rb') as f:
        multiprocessing.current_process().authkey = b'myauth'
        backup = pickle.load(f)
        my_print(Verbosity.all, "Unpickled the server object")
        
    backup.assume_backup_role()
    my_print(Verbosity.all, "Assumed backup server role")
    my_print(Verbosity.instance_creation_etc, 
            f"to_client_id={backup.to_client_id}")
    backup.run()