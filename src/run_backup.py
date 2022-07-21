from src.constants import Constants
import multiprocessing
import pickle

print("Running backup server", flush=True)
with open(Constants.PICKLED_SERVER_FILE, 'rb') as f:
    multiprocessing.current_process().authkey = b'myauth'
    backup = pickle.load(f)
    print("Unpickled the server object", flush=True)
backup.assume_backup_role()
print("Assumed backup server role", flush=True)
backup.run()