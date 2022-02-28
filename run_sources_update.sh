#!/bin/bash

cd $DLG_CLICKSTREAM

printerr(){
	echo $1 >> /dev/stderr
}

printerr "%%%%% START lib/sources_update.py %%%%%"
lib/sources_update.py