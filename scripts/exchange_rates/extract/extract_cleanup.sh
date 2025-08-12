#!/bin/bash

HOME_DIR='/root/Development/Airflow'
DATA_DIR=$HOME_DIR'/data'
SCRIPTS_DIR=$HOME_DIR'/scripts'
DATA_BKP_DIR=$DATA_DIR'/backup'

echo $DATA_DIR
ls $DATA_DIR

find  $DATA_DIR -type f -print0 -maxdepth 1 | xargs -0 -I {}  mv "{}" /root/Development/Airflow/data/backup/

# find $DATA_BKP_DIR -type f -mtime +14 | xargs rm