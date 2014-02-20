/**
 *  This script is an example recommender (using data from http://www.informatik.uni-freiburg.de/~cziegler/BX/)
 *  that demonstrates the add item-item links technique.  The item-item links are generated based on existing
 *  common traits in the set of items. In this case, item-item links are generated based on the author of the
 *  book, where a link is generated when two books have the same author.  
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

/****** Changes for diversifying items ********/

inventory_input = LOAD '$INPUT_PATH_INVENTORY' USING org.apache.pig.piggybank.storage.JsonLoader(
                     'movie_title: chararray, 
                      genres: bag{tuple(content:chararray)}');
-- Generate metadata that is a vital arguement for buildig recommendations
metadata = FOREACH inventory_input GENERATE
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
