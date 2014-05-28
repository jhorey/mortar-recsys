#! /bin/bash

function run_as_ferry {
    echo -e "${GREEN} ${2} ${NC}"
    if [ $USER == "root" ]; then
	su ferry -c "$1"
    else
	$1
    fi
}

if [ $1 == "data" ]; then
    MKDIR='hadoop dfs -mkdir -p /service/data/retail'
    COPY1='hadoop dfs -copyFromLocal /home/ferry/mortar-recsys/data/retail/purchases.json /service/data/retail/'
    COPY2='hadoop dfs -copyFromLocal /home/ferry/mortar-recsys/data/retail/wishlists.json /service/data/retail/'
    COPY3='hadoop dfs -copyFromLocal /home/ferry/mortar-recsys/data/retail/inventory.json /service/data/retail/'

    run_as_ferry "$MKDIR" "Making data directory"
    run_as_ferry "$COPY1" "Copy purchases dataset"
    run_as_ferry "$COPY2" "Copy wishlists dataset"
    run_as_ferry "$COPY3" "Copy inventory dataset"
elif [ $1 == "retail" ]; then
    PIG='pig -f pigscripts/retail-recsys.pig -m params/retail.params'
    run_as_ferry "$PIG" "Running retail script"
fi