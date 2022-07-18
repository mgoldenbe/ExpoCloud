from src.constants import Constants
import multiprocessing
import pickle

with open(Constants.PICKLED_SERVER_FILE, 'rb') as f:
    multiprocessing.current_process().authkey = b'myauth'
    backup = pickle.load(f)
backup.assume_backup_role()
backup.run()