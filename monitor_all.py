import threading
import subprocess
import json
import time

metadata = json.load(open("monitor_all.json"))

def call_plotting(name,target_komp,pid,counter,extra,duration):
    filename="plot_{}_{}_{}_{}.png".format(
        name,extra,target_komp,counter
    )

    subprocess.Popen(
        "ssh aaron@{} \"source .bashrc;/home/aaron/.local/bin/psrecord {} --plot {} --interval 1 --duration {}\"".format(target_komp, pid, filename, duration),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    ).wait()
    
    subprocess.Popen(
        'scp aaron@{}:~/{} \.'.format(target_komp, filename),
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    ).wait()

workers = []
for i,komp in enumerate(metadata['komp']):
    for j,pid in enumerate(metadata['pid'][i]):
        t=threading.Thread(target=call_plotting,args=(
            metadata['name'],               # name of database
            komp,                           # what komp to ssh to
            pid,                            # what pid to check
            'num'+str(metadata['counter']), # what test number for this setting is
            str(j)+'-'+metadata['extra'],   # setting to be written as description
            metadata['duration']            # how long to record
        ))
        workers.append(t)

[w.start() for w in workers]
[w.join() for w in workers]