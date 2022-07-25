from src.constants import Constants
import multiprocessing
import pickle
from src import util
import time

print("Running backup server", flush=True)
with open(util.pickled_file_name(util.command_arg_name()), 'rb') as f:
    multiprocessing.current_process().authkey = b'myauth'
    backup = pickle.load(f)
    print("Unpickled the server object", flush=True)
backup.assume_backup_role()
print("Assumed backup server role", flush=True)
backup.run()