/**
 *  This script is an example recommender (using made up data) showing how you might extract 
 *  multiple user-item signals from your data.  Here, we extract one signal based on
 *  user purchase information and another based on a user adding a movie to their wishlist.  We
 *  then combine those signals before running the Mortar recommendation system to get item-item
 *  and user-item recommendations.
 */
import 'recommenders.pig';

%default INPUT_PATH_PURCHASES 's3://cngan-dev/retail/purchases.json'
%default INPUT_PATH_WISHLIST 's3://cngan-dev/retail/wishlists.json'
%default OUTPUT_PATH 's3://cngan-dev/data/retail/out/diversifyUI'


/******* Load Data **********/

--Get purchase signals
purchase_input = LOAD '$INPUT_PATH_PURCHASES' USING org.apache.pig.piggybank.storage.JsonLoader(
                    'row_id: int, 
                     movie_id: chararray, 
                     movie_name: chararray, 
                     user_id: chararray, 
                     purchase_price: int');

--Get wishlist signals
wishlist_input =  LOAD '$INPUT_PATH_WISHLIST' USING org.apache.pig.piggybank.storage.JsonLoader(
                     'row_id: int, 
                      movie_id: chararray, 
                      movie_name: chararray, 
                      user_id: chararray');



/******* Convert Data to Signals **********/

-- Start with choosing 1 as max weight for a signal.
purchase_signals = FOREACH purchase_input GENERATE
                        user_id    as user,
                        movie_name as item,
                        1.0        as weight; 


-- Start with choosing 0.5 as weight for wishlist items because that is a weaker signal than
-- purchasing an item.
wishlist_signals = FOREACH wishlist_input GENERATE
                        user_id    as user,
                        movie_name as item,
                        0.5        as weight; 

user_signals = UNION purchase_signals, wishlist_signals;

user_signals_filt = FILTER user_signals BY (user is not null) AND (item is not null);

/******* Use Mortar recommendation engine to convert signals to recommendations **********/

item_item_recs = recsys__GetItemItemRecommendations(user_signals);
item_item_recs_filt = FILTER item_item_recs BY  (item_B is not null) 
                                                AND (item_A is not null) 
                                                AND (weight is not null) 
                                                AND (raw_weight is not null) 
                                                AND (rank is not null);

user_item_recs = recsys__GetUserItemRecommendations(user_signals, item_item_recs_filt);


/******* Store recommendations **********/

--  If your output folder exists already, hadoop will refuse to write data to it.
rmf $OUTPUT_PATH/item_item_recs;
rmf $OUTPUT_PATH/user_item_recs;

store item_item_recs into '$OUTPUT_PATH/item_item_recs' using PigStorage();
store user_item_recs into '$OUTPUT_PATH/user_item_recs' using PigStorage();
