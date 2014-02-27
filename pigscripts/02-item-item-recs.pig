import 'recommenders.pig';

%default OUTPUT_PATH 's3://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/retail-recommendations-luigi'

user_signals = load '$OUTPUT_PATH/user_signals' using PigStorage()
                        as (user: chararray, item: chararray, weight: float);


-- Be sure to include any custom modifications.

item_item_recs = recsys__GetItemItemRecommendations(user_signals);


rmf $OUTPUT_PATH/item_item_recs;

store item_item_recs into '$OUTPUT_PATH/item_item_recs' using PigStorage();
