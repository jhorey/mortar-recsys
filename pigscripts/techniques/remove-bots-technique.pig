/**
 *  This script is an example recommender (from made up data) 
 *  that demonstrates the remove bots technique.  Bots are users that have an abnormally high amount of
 *  generated signals such that it ultimately disrupts user-item signals in your set of data.  
 *  Make sure that this script is ran using the 'techniques.param' file for the parameter file
 *  as a THREHOLD parameter is set in that instance.
 */
import 'recommenders.pig';

%default INPUT_PATH_PURCHASES '../data/retail/purchases.json'
%default INPUT_PATH_WISHLIST '../data/retail/wishlists.json'
%default OUTPUT_PATH '../data/retail/out/remove_bots'


/******* Load Data **********/

--Get purchase signals
purchase_input = load '$INPUT_PATH_PURCHASES' using org.apache.pig.piggybank.storage.JsonLoader(
                    'row_id: int, 
                     movie_id: chararray, 
                     movie_name: chararray, 
                     user_id: chararray, 
                     purchase_price: int');

--Get wishlist signals
wishlist_input =  load '$INPUT_PATH_WISHLIST' using org.apache.pig.piggybank.storage.JsonLoader(
                     'row_id: int, 
                      movie_id: chararray, 
                      movie_name: chararray, 
                      user_id: chararray');


/******* Convert Data to Signals **********/

-- Start with choosing 1 as max weight for a signal.
purchase_signals = foreach purchase_input generate
                        user_id    as user,
                        movie_name as item,
                        1.0        as weight; 


-- Start with choosing 0.5 as weight for wishlist items because that is a weaker signal than
-- purchasing an item.
wishlist_signals = foreach wishlist_input generate
                        user_id    as user,
                        movie_name as item,
                        0.5        as weight; 

user_signals = union purchase_signals, wishlist_signals;



/********* Remove user-item signals that are considered bots **********/

-- Threshold is set in param file
user_signals_removed_bots = recsys__RemoveBots(user_signals, $THRESHOLD);



/******* Use Mortar recommendation engine to convert signals to recommendations **********/
item_item_recs_remove= recsys__GetItemItemRecommendations(user_signals_removed_bots); -- Use Filtered Data 
user_item_recs_remove = recsys__GetUserItemRecommendations(user_signals, item_item_recs_remove);



/******* Store recommendations **********/

--dump user_item_recs_II_filt;
rmf $OUTPUT_PATH/item_item_recs;
rmf $OUTPUT_PATH/user_item_recs;

store item_item_recs_remove into '$OUTPUT_PATH/item_item_recs' using PigStorage();
store user_item_recs_remove into '$OUTPUT_PATH/user_item_recs' using PigStorage();

