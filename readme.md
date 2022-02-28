# Digital Lead Generation Pipeline for processing Google Clickstream using Pysprak with Hive support 
*PROJECT STRUCTURE*:
- `NRT_pipeline`:
    - `*.py`: .py scripts for real time consuming data from `HDFS` using `Kafka2HDFS` mechanism 
    - `runAllScripts.sh`: main shell runner
- `scenarios/`: collection of .py scripts for Offline consuming `HDFS` data following with data preprocessing via pysprak
- `lib`:
    - `new_data_slice.py`: make data slice from external `HDFS` store 
    - `scenario_base.py`:
    - `sources_update.py`: data updater: load necessary table from `Oracle`
    - `scenario_stats.py`: calculate overall statistics after finishing all clickstream scenarios in `/scenarios/` 
    - `ga_all_snenarios_insert.py`: script for inserting processed data into Hive
    - `mail_sender.py`: class for auto-emailing and message broadcasting
    - `tools.py`: config with global variables/json definition 
    - `export_to_iskra.py`: jDBC data loader from Hive to Oracle
- `Hive_External_Tbl`: pyspark script for working with external `HDFS` partitions with `HIVEQL` script for partition auto sync