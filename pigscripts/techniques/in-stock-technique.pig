/**
 *  This script is an example recommender (using made up data) showing how you can create recommendations
 *  from determining if an item is in stock or not.  This information must be determined by metadata
 *  that previously exists.
 */
import 'macros/recommenders.pig';

%default INPUT_PATH_PURCHASES '/service/data/retail/purchases.json'
%default INPUT_PATH_WISHLIST '/service/data/retail/wishlists.json'
%default INPUT_PATH_INVENTORY '/service/data/retail/inventory.json'
%default OUTPUT_PATH '/service/data/retail/out/in_stock'


/******* Load Data **********/

--Get purchase signals
purchase_input = load '$INPUT_PATH_PURCHASES' using org.apache.pig.builtin.JsonLoader(
                    'movie_id: chararray, 
                     row_id: int, 
		     user_id: chararray, 
		     purchase_price: int,
                     movie_name: chararray');

--Get wishlist signals
wishlist_input =  load '$INPUT_PATH_WISHLIST' using org.apache.pig.builtin.JsonLoader(
                     'movie_id: chararray, 
		      row_id: int,                     
		      user_id: chararray,
                      movie_name: chararray');


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

/******** Changes for Consideration of Items in Stock  ******/
inventory_input = load '$INPUT_PATH_INVENTORY' using org.apache.pig.builtin.JsonLoader(
                     'genres: bag{tuple(content:chararray)},
		      movie_title: chararray,
                      stock: int');
                      

-- recsys__GetItemItemRecommendations_WithAvailableItems utilizes source_items to have schema as such
-- where the item is the only field
available_items = foreach (filter inventory_input by stock > 0) generate
                      movie_title as item;


/******* Use Mortar recommendation engine to convert signals to recommendations **********/

-- Use of non standard Mortar Recommendation engine macro
item_item_recs = recsys__GetItemItemRecommendations_WithAvailableItems(user_signals, available_items, available_items);
user_item_recs = recsys__GetUserItemRecommendations(user_signals, item_item_recs);


/******* Store recommendations **********/

--  If your output folder exists already, hadoop will refuse to write data to it.
rmf $OUTPUT_PATH/item_item_recs;
rmf $OUTPUT_PATH/user_item_recs;

store item_item_recs into '$OUTPUT_PATH/item_item_recs' using PigStorage();
store user_item_recs into '$OUTPUT_PATH/user_item_recs' using PigStorage();
