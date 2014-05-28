#! /bin/bash

# Bash colors
GREEN='\e[0;32m'
NC='\e[0m'

function run_as_ferry {
    echo -e "${GREEN} ${2} ${NC}"
    if [ $USER == "root" ]; then
	su ferry -c "$1"
    else
	$1
    fi
}

# Make the output directory
MKDIR='mkdir -p /service/data/out'
run_as_ferry "$MKDIR" "Creating output directories"

# Navigate to the Mortar directory.
cd /home/ferry/mortar-recsys

if [ $1 == "data" ]; then
    MKDIR='hdfs dfs -mkdir -p /service/data/retail'
    COPY1='hdfs dfs -copyFromLocal /home/ferry/mortar-recsys/data/retail/purchases.json /service/data/retail/'
    COPY2='hdfs dfs -copyFromLocal /home/ferry/mortar-recsys/data/retail/wishlists.json /service/data/retail/'
    COPY3='hdfs dfs -copyFromLocal /home/ferry/mortar-recsys/data/retail/inventory.json /service/data/retail/'

    run_as_ferry "$MKDIR" "Making data directory"
    run_as_ferry "$COPY1" "Copy purchases dataset"
    run_as_ferry "$COPY2" "Copy wishlists dataset"
    run_as_ferry "$COPY3" "Copy inventory dataset"
elif [ $1 == "retail" ]; then
    PIG='$PIG_HOME/bin/pig -f pigscripts/retail-recsys.pig -m params/retail.params'
    run_as_ferry "$PIG" "Running retail script"
elif [ $1 == "users" ]; then
    COPY='hdfs dfs -copyToLocal /service/data/retail/out/user_item_recs /service/data/out/'
    run_as_ferry "$COPY" "Copying user recommendations"
elif [ $1 == "items" ]; then
    COPY='hdfs dfs -copyToLocal /service/data/retail/out/item_item_recs /service/data/out/'
    run_as_ferry "$COPY" "Copying item recommendations"
fi