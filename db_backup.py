import os
import subprocess
from datetime import date

backup_path = "/propaganda_backups/"
max_files = 62 # keeps state of DB 2 months back

def sorted_ls(path):
    mtime = lambda f: os.stat(os.path.join(path, f)).st_mtime
    return list(sorted(os.listdir(path), key=mtime))

def check_and_clean_backups():

    del_list = sorted_ls(backup_path)[0:(len(sorted_ls(backup_path))-max_files)]

    for dfile in del_list:
        #print(backup_path + dfile)
        os.remove(backup_path + dfile)

check_and_clean_backups()

DB_NAME = 'pybossa'  #  db name

DB_USER = 'root' #  db user
DB_HOST = "localhost"
DB_PASSWORD = '1234'#  db password
FILENAME = 'default_backup.dmp'

today = date.today()
curr_time = today.strftime("%Y_%m_%d")
FILENAME = '/propaganda_backups/pybossa_propaganda_backup_' + curr_time + '.dmp'
dump_success = 1
print ('Backing up %s database ' % (DB_NAME))
command = f'pg_dump --host={DB_HOST} ' \
            f'--dbname={DB_NAME} ' \
            f'--username={DB_USER} ' \
            f'--no-password ' \
            f'--file={FILENAME} ' \
            f'--verbose ' \
            f'--format=c ' \
            f'--blobs'
try:
    proc = subprocess.Popen(command, shell=True, env={
                'PGPASSWORD': DB_PASSWORD
                })
    proc.wait()

except Exception as e:
    dump_success = 0
    print('Exception happened during dump %s' %(e))


if dump_success:
    print('DB backup was created successfull')
    #print(' restoring to a new database database')
