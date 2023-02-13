#!/usr/bin/python
import subprocess

command = 'docker exec pybossa python3 /propaganda_pg_dump.py'

try:
    proc = subprocess.Popen(command, shell=True)
    proc.wait()

except Exception as e:
    dump_success = 0
    print('Exception happened during dump %s' %(e))

command='docker cp pybossa:/propaganda_backups/. /data/didimitrov/propaganda_backups/.'

try:
    proc = subprocess.Popen(command, shell=True)
    proc.wait()

except Exception as e:
    dump_success = 0
    print('Exception happened during dump %s' %(e))
