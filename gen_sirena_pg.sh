#!/bin/bash
SIRENA_DIR=$1
SIRENA_PG=$2
TABLE_NAME=$3
SCRIPT_PATH=$(dirname "$0")

$SCRIPT_PATH/gen_pg_tabs.py $SIRENA_PG -l $TABLE_NAME,ARC_$TABLE_NAME,SEQ_$TABLE_NAME -d $SIRENA_DIR/sql/bases/sirena_pg -a $SIRENA_DIR/sql/bases/sirena_pg/bases_pg/archive -a1 1Tab_other -a1p ARCHIVE.PG_ -tis -v
