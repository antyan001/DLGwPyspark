
#!/bin/bash
#PASS=$(cat /home/ektov1-av_ca-sbrf-ru/pass/pswrd | sed 's/\r//g'); echo $PASS | kinit
cd /home/$USER/notebooks/NRT_RUN/
echo "!!!RUN STEPBYSTEP SCRIPTS FOR EACH SCENARIO/-->"
echo "working with script NRT_CORPCARD.py..."
./NRT_CORPCARD.py
echo "working with script NRT_ACQUIRING.py..."
./NRT_ACQUIRING.py
echo "working with script NRT_CREDIT.py..."
./NRT_CREDIT.py
echo "!!!EXPORT HIVE TO ORACLE-->"
echo "working with script export2iskra_nrt_all_scenarios_hist.py..."
./export2iskra_nrt_all_scenarios_hist.py
