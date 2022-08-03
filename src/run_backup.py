from src.constants import Constants
import multiprocessing
import pickle

from src.util import myprint
from src.constants import Verbosity

from src import util
import time

if __name__ == '__main__':
    myprint(Verbosity.all, "Running backup server")
    path = util.output_folder(util.command_arg_name())
    with open(util.pickled_file_name(path), 'rb') as f:
        multiprocessing.current_process().authkey = b'myauth'
        backup = pickle.load(f)
        myprint(Verbosity.all, "Unpickled the server object")
        
    backup.assume_backup_role()
    myprint(Verbosity.all, "Assumed backup server role")
    myprint(Verbosity.instance_creation_etc, 
            f"to_client_id={backup.to_client_id}")
    backup.run()