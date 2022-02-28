#!/bin/bash

cd $DLG_CLICKSTREAM

printerr(){
	echo $1 >> /dev/stderr
}

#########################################
########### PREPARE RESOURCES############
#########################################

printerr "%%%%% START lib/new_data_slice.py %%%%%"
lib/new_data_slice.py

# printerr "%%%%% START lib/cid_sbbol_inn_update.py %%%%%"
# lib/cid_sbbol_inn_update.py

#printerr "%%%%% START lib/LoadDCCVMIDMap.py %%%%%"
#lib/LoadDCCVMIDMap.py

#########################################
############### SCENARIOS ###############
#########################################

printerr "%%%%% START scenarios/ACQUIRING_01.py %%%%%"
scenarios/ACQUIRING_01.py

printerr "%%%%% START scenarios/BUSINESS_CARD_01.py %%%%%"
scenarios/BUSINESS_CARD_01.py

printerr "%%%%% START scenarios/CASHORDER_01.py %%%%%"
scenarios/CASHORDER_01.py

printerr "%%%%% START scenarios/CORPCARD_01.py %%%%%"
scenarios/CORPCARD_01.py

printerr "%%%%% START scenarios/CORPCARD_02.py %%%%%"
scenarios/CORPCARD_02.py

printerr "%%%%% START scenarios/CREDIT_02.py %%%%%"
scenarios/CREDIT_02.py

printerr "%%%%% START scenarios/DEPOSIT_01.py %%%%%"
scenarios/DEPOSIT_01.py

printerr "%%%%% START scenarios/DIGITALCARD_01.py %%%%%"
scenarios/DIGITALCARD_01.py

printerr "%%%%% START scenarios/DIGITALCARD_02.py %%%%%"
scenarios/DIGITALCARD_02.py

printerr "%%%%% START scenarios/ENCASHMENT_01.py %%%%%"
scenarios/ENCASHMENT_01.py

printerr "%%%%% START scenarios/HELP_SBBOL.py %%%%%"
scenarios/HELP_SBBOL.py

printerr "%%%%% START scenarios/LETTERSOFCREDIT_01.py %%%%%"
scenarios/LETTERSOFCREDIT_01.py

printerr "%%%%% START scenarios/PREMIUMCARD_01.py %%%%%"
scenarios/PREMIUMCARD_01.py

printerr "%%%%% START scenarios/PREMIUMCARD_02.py %%%%%"
scenarios/PREMIUMCARD_02.py

printerr "%%%%% START scenarios/PREMIUMCARDMASTER_01.py %%%%%"
scenarios/PREMIUMCARDMASTER_01.py

printerr "%%%%% START scenarios/PREMIUMCARDMASTER_02.py %%%%%"
scenarios/PREMIUMCARDMASTER_02.py

printerr "%%%%% START scenarios/SHOP_01.py %%%%%"
scenarios/SHOP_01.py

printerr "%%%%% START scenarios/STORIES_01.py %%%%%"
scenarios/STORIES_01.py

printerr "%%%%% START scenarios/WTA_SITE_NBS.py %%%%%"
scenarios/WTA_SITE_NBS.py

printerr "%%%%% START scenarios/COLLINSUR_ONLINE.py %%%%%"
scenarios/COLLINSUR_ONLINE.py

printerr "%%%%% START scenarios/COLLINSUR_OTHER.py %%%%%"
scenarios/COLLINSUR_OTHER.py

printerr "%%%%% START scenarios/COLLINSUR_ALREADY.py %%%%%"
scenarios/COLLINSUR_ALREADY.py

printerr "%%%%% START scenarios/RKO_OPEN_ACC.py %%%%%"
scenarios/RKO_OPEN_ACC.py

printerr "%%%%% START scenarios/RKO_DOS.py %%%%%"
scenarios/RKO_DOS.py

printerr "%%%%% START scenarios/RKO_BANK.py %%%%%"
scenarios/RKO_BANK.py

printerr "%%%%% START lib/ga_site_all_prods_update.py %%%%%"
lib/ga_site_all_prods_update.py

printerr "%%%%% START scenarios/SITEALLPRODAGG_01.py %%%%%"
scenarios/SITEALLPRODAGG_01.py


#########################################
########### STATS AND EXPORT ############
#########################################

printerr "%%%%% START lib/export_to_iskra.py %%%%%"
lib/export_to_iskra.py 

printerr "%%%%% START lib/scenario_stats.py %%%%%"
lib/scenario_stats.py

#########################################
############ HISTORY INSERT #############
#########################################

printerr "%%%%% START lib/ga_all_snenarios_insert.py %%%%%"
lib/ga_all_snenarios_insert.py
