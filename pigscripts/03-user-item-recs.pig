import 'recommenders.pig';

%default OUTPUT_PATH 's3://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/retail-recommendations-luigi'

user_signals    =   load '$OUTPUT_PATH/user_signals' using PigStorage()
                        as (user: chararray, item: chararray, weight: float);

item_item_recs  =   load '$OUTPUT_PATH/item_item_recs' using PigStorage()
                        as (item_A:chararray, item_B:chararray, weight:float, raw_weight:float, rank:int);

-- Be sure to include any custom modifications.

user_item_recs = recsys__GetUserItemRecommendations(user_signals, item_item_recs);

rmf $OUTPUT_PATH/user_item_recs;

store user_item_recs into '$OUTPUT_PATH/user_item_recs' using PigStorage();
