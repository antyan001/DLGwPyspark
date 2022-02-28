#!/usr/bin/sh
lsoffree=$(lsof | grep -E "runAllScripts.sh|NRT.*py|export2iskra_nrt_all_scenarios_hist*" | awk '{print $2}' | xargs echo kill -9 | bash); $lsoffree
clearvar=$(ps -ef | grep -E "runAllScripts.sh|NRT.*py|export2iskra_nrt_all_scenarios_hist*" | grep -v "grep" | grep -v "restart" | awk '{print $2}' | xargs echo kill -9 | bash); $clearvar
