item_item_recs = LOAD '$OUTPUT_PATH/item_item_recs'
    USING PigStorage()
    AS (from_id:chararray, to_id:chararray, weight:float, raw_weight:float, rank:int);

user_item_recs = LOAD '$OUTPUT_PATH/user_item_recs'
    USING PigStorage()
    AS (from_id:chararray, to_id:chararray, weight:float, reason_item:chararray, 
        user_reason_item_weight:float, item_reason_item_weight:float, rank:int);

store item_item_recs  into '$CONN/$DB.$II_COLLECTION'
                     using com.mongodb.hadoop.pig.MongoInsertStorage('','');

store user_item_recs  into '$CONN/$DB.$UI_COLLECTION'
                     using com.mongodb.hadoop.pig.MongoInsertStorage('','');
