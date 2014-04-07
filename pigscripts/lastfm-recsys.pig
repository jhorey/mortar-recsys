import 'recommenders.pig';

/**
 *  Generates artist recommendations based off of last.fm data provided by 
 *  http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-360K.html
 */

%default INPUT_SIGNALS 's3://mortar-example-data/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv'
%default OUTPUT_PATH 's3://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/lastfm-recommendations'

input_signals  =    load '$INPUT_SIGNALS' using PigStorage()
                        as (user: chararray, item_id: chararray, item: chararray, weight: float);


item_item_recs =    recsys__GetItemItemRecommendations(input_signals);
user_item_recs =    recsys__GetUserItemRecommendations(input_signals, item_item_recs);

--  If your output folder exists already, hadoop will refuse to write data to it.
rmf $OUTPUT_PATH/item_item_recs;
rmf $OUTPUT_PATH/user_item_recs;

store item_item_recs into '$OUTPUT_PATH/item_item_recs' using PigStorage();
store user_item_recs into '$OUTPUT_PATH/user_item_recs' using PigStorage();
