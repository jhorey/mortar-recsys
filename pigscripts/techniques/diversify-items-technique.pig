/**
 *  This script is an example recommender (using made up data), which extends the retail example 
 *  to demonstrate the diversify items technique. This script creates metadata from the inventory
 *  dataset where titles are the items and the genre is the metadata_field.  The metadata entity
 *  is then passed as an arguement to automatically create more diverse recommendations. 
 */
import 'recommenders.pig';



/*
 * Diversify Items Technique
*/
%default INPUT_PATH_PURCHASES '../data/retail/purchases.json'
%default INPUT_PATH_WISHLIST '../data/retail/wishlists.json'
%default INPUT_PATH_INVENTORY '../data/retail/inventory.json' -- added on for techniques

%default OUTPUT_PATH '../data/retail/out/diversify'


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

/****** Changes for diversifying items ********/

inventory_input = load '$INPUT_PATH_INVENTORY' using org.apache.pig.piggybank.storage.JsonLoader(
                     'movie_title: chararray, 
                      genres: bag{tuple(content:chararray)}');

-- Generate metadata that is a vital arguement for buildig recommendations
metadata = foreach inventory_input generate
                          FLATTEN(genres) as metadata_field,
                          movie_title as item;


item_item_recs = recsys__GetItemItemRecommendations_DiversifyItemItem(user_signals, metadata);

/******  Utilization of standard recsys code *******/
user_item_recs = recsys__GetUserItemRecommendations(user_signals, item_item_recs);


/******* Store recommendations **********/

--  If your output folder exists already, hadoop will refuse to write data to it.
rmf $OUTPUT_PATH/item_item_recs;
rmf $OUTPUT_PATH/user_item_recs;

store item_item_recs into '$OUTPUT_PATH/item_item_recs' using PigStorage();
store user_item_recs into '$OUTPUT_PATH/user_item_recs' using PigStorage();
