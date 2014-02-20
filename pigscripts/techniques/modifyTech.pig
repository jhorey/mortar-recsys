/**
 *  This script is an example recommender (using made up data) showing how you might modify item-item links
 *  by defining similar relations between datat, the metadatat, and customizing the change in weighting. 
 *  This technique requires a customization of the standard GetItemItemRecommendations macro
 */
import 'recommenders.pig';


/******** Custom GetItemItemRecommnedations *********/
define recsys__GetItemItemRecommendations_ModifyCustom(user_item_signals, metadata) returns item_item_recs {

    -- Convert user_item_signals to an item_item_graph
    ii_links_raw, item_weights   =   recsys__BuildItemItemGraph(
                                       $user_item_signals,
                                       $LOGISTIC_PARAM,
                                       $MIN_LINK_WEIGHT,
                                       $MAX_LINKS_PER_USER
                                     );
    -- NOTE this function is added in order to combine metadata with item-item links
        -- See macro for more detailed explination
    ii_links_metadata           =   recsys__PutMetadataToItemItemLinks(
                                        ii_links_raw, 
                                        $metadata
                                    ); 
 
    /********* Custom Code starts here ********/
    
    --The code here should adjust the weights based on an item-item link and the equality of metadata.
    -- In this case, if the metadata is the same, the weight is reduced.  Otherwise the weight is left alone.
    ii_links_adjusted           =  FOREACH ii_links_metadata GENERATE item_A, item_B,
                                        -- the amount of weight adjusted is dependant on the domain of data and what is expected
                                        (metadata_B == metadata_A ? (weight - 0.5): weight) as weight; 


    /******** Custom Code stops here *********/
    -- TODO we need to recalculate the overall_item weights as it is changed after modifying it with meta data
    -- Adjust the weights of the graph to improve recommendations.
    ii_links                    =   recsys__AdjustItemItemGraphWeight(
                                        ii_links_adjusted,
                                        item_weights,
                                        $BAYESIAN_PRIOR
                                    );

    -- Use the item-item graph to create item-item recommendations.
    $item_item_recs =  recsys__BuildItemItemRecommendationsFromGraph(
                           ii_links,
                           $NUM_RECS_PER_ITEM, 
                           $NUM_RECS_PER_ITEM
                       );
};







%default INPUT_PATH_PURCHASES '../data/retail/purchases.json'
%default INPUT_PATH_WISHLIST '../data/retail/wishlists.json'
%default INPUT_PATH_INVENTORY '../data/retail/inventory.json'
%default OUTPUT_PATH '../data/retail/out/modify'


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


/******** Changes for Modifying item-item links ******/
inventory_input = LOAD '$INPUT_PATH_INVENTORY' USING org.apache.pig.piggybank.storage.JsonLoader(
                     'movie_title: chararray, 
                      genres: bag{tuple(content:chararray)}');


metadata = FOREACH inventory_input GENERATE
              FlATTEN(genres) as metadata_field,
              movie_title as item;
-- requires the macro to be written seperately
item_item_recs = recsys__GetItemItemRecommendations_ModifyCustom(user_signals, metadata);
user_item_recs = recsys__GetUserItemRecommendations(user_signals, item_item_recs);

/******* Store recommendations **********/

--  If your output folder exists already, hadoop will refuse to write data to it.

rmf $OUTPUT_PATH/item_item_recs;
rmf $OUTPUT_PATH/user_item_recs;

store item_item_recs into '$OUTPUT_PATH/item_item_recs' using PigStorage();
store user_item_recs into '$OUTPUT_PATH/user_item_recs' using PigStorage();



