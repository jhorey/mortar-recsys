/*
 * Copyright 2014 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "as is" Basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import 'recsys.pig';
import 'recsys_alternatives.pig';
import 'recommender_alternatives.pig';
import 'recsys_util.pig';
/*
 * This macro will create item-to-item recommendations based on user-item signals. 
 * 
 * Input:
 *      user_item_signals: { (user:chararray, item:chararray, weight:float) }
 *
 * Output:
 *      item_item_recs: { (item_A:chararray, item_B:chararray, weight:flaot, raw_weight:float, rank:int) }
 */
define recsys__GetItemItemRecommendations(user_item_signals) returns item_item_recs {

    -- Convert user_item_signals to an item_item_graph
    ii_links_raw, item_weights   =   recsys__BuildItemItemGraph(
                                       $user_item_signals,
                                       $LOGISTIC_PARAM,
                                       $MIN_LINK_WEIGHT,
                                       $MAX_LINKS_PER_USER
                                     );

    -- Adjust the weights of the graph to improve recommendations.
    ii_links                    =   recsys__AdjustItemItemGraphWeight(
                                        ii_links_raw,
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


/*
 * This macro will create user-to-item recommendations based on user-item signals and 
 * a set of previously constructed item-to-item recommmendations.
 * 
 * Input:  
 *      user_item_signals: { (user:chararray, item:chararray, weight:float) }
 *      item_item_recs: { (item_A:chararray, item_B:chararray, weight:float) }
 *
 * Output: 
 *      user_item_recs: { (user:chararray, item:chararray, weight:flaot, reason_item:chararray, 
 *                         user_reason_item_weight:float, item_reason_item_weight:float, rank:int) }
 *
 *      reason_item: The item the user interacted with that generated this recommendation
 *      user_reason_item_weight: The weight the user had with the reason_item
 *      item_reason_item_weight: The original weight the item recommended had with the reason_item 
 */
define recsys__GetUserItemRecommendations(user_item_signals, item_item_recs) returns user_item_recs {

    $user_item_recs = recsys__BuildUserItemRecommendations(
                            $user_item_signals,
                            $item_item_recs,
                            $NUM_RECS_PER_USER,
                            '$ADD_DIVERSITY_FACTOR'
                      );
};

