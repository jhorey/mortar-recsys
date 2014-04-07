import 'recommenders.pig';

%default INPUT_PATH 's3://my-bucket/input/my-data-file-or-directory'
%default OUTPUT_PATH 's3://my-bucket/output'


/******* Load Data **********/

/*
-- For help figuring out how to load your data visit help.mortardata.com/reference/loading_and_storing_data/load_statement_generator
raw_input = load '$INPUT_PATH' using PigStorage()
                as (user: chararray, item: chararray, date_purchased: chararray);
*/


/******* Convert Data to Signals **********/
/*
-- Create (user, item, weight) tuples for your data to be used as input to the Mortar recommender.
user_signals = foreach raw_input generate
                 user as user,
                 item as item,
                 1.0  as weight; -- Arbitrarily choose 1 as weight for purchasing an item.
*/


/******* Use Mortar recommendation engine to convert signals to recommendations **********/
/*
-- Call the default Mortar recommender algorithm on your user-item data.
-- The input_signals alias needs to have the following fields: (user, item, weight:float)
item_item_recs = recsys__GetItemItemRecommendations(user_signals);
user_item_recs = recsys__GetUserItemRecommendations(user_signals, item_item_recs);


--  If your output folder exists already, hadoop will refuse to write data to it. 
rmf $OUTPUT_PATH/item_item_recs;
rmf $OUTPUT_PATH/user_item_recs;

store item_item_recs into '$OUTPUT_PATH/item_item_recs' using PigStorage();
store user_item_recs into '$OUTPUT_PATH/user_item_recs' using PigStorage();
*/
