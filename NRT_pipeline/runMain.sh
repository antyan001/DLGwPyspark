#!/bin/bash
cd /home/$USER/notebooks/NRT_RUN/
./exitingRunningProcesses.sh > /dev/null 2>&1 && ./runAllScripts.sh


