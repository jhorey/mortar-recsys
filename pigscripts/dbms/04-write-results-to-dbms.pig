
item_item_recs = LOAD '$OUTPUT_PATH/item_item_recs'
    USING PigStorage()
    AS (from_id:chararray, to_id:chararray, weight:float, raw_weight:float, rank:int);

user_item_recs = LOAD '$OUTPUT_PATH/user_item_recs'
    USING PigStorage()
    AS (from_id:chararray, to_id:chararray, weight:float, reason_item:chararray,
        user_reason_item_weight:float, item_reason_item_weight:float, rank:int);

store item_item_recs  into 'hdfs:///unused-ignore'
   USING org.apache.pig.piggybank.storage.DBStorage('$DATABASE_DRIVER',
   'jdbc:$DATABASE_TYPE://$DATABASE_HOST/$DATABASE_NAME',
   '$DATABASE_USER',
   '$DATABASE_PASS',
   'INSERT INTO $II_TABLE(from_id,to_id,weight,raw_weight,rank) VALUES (?,?,?,?,?)');

store user_item_recs  into 'hdfs:///unused-ignore'
   USING org.apache.pig.piggybank.storage.DBStorage('$DATABASE_DRIVER',
   'jdbc:$DATABASE_TYPE://$DATABASE_HOST/$DATABASE_NAME',
   '$DATABASE_USER',
   '$DATABASE_PASS',
   'INSERT INTO $UI_TABLE(from_id,to_id,weight,reason_item,user_reason_item_weight,item_reason_item_weight,rank) VALUES (?,?,?,?,?,?,?)');
