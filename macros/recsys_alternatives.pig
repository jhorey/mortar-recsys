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

register 'datafu-0.0.10.jar';
register 'trove4j-3.0.3.jar';
register 'recsys-udfs.jar';

define recsys__Enumerate 
    datafu.pig.bags.Enumerate('1');

register 'recsys.py' using jython as recsys_udfs;

----------------------------------------------------------------------------------------------------
/*
 * This file contains macros that can be used to modify the standard Mortar recommendation system
 * in macros/recommenders.pig.
 */
----------------------------------------------------------------------------------------------------


/*
 * This is an alternative to recsys__AdjustItemItemGraphWeight.  This version boosts more popular items
 * to increase the chance that they are recommended.  
 *
 * Input:
 *     Same inputs as recsys__AdjustItemItemGraphWeight
 *     pop_boost_func: 'SQRT', 'LOG', ''(linear).
 *
 * Ouptut:
 *     Same output as recsys__AdjustItemItemGraphWeight
 */
define recsys__AdjustItemItemGraphWeight_withPopularityBoost(
                            ii_links_raw, item_weights, prior, pop_boost_func)
returns ii_links_bayes {

    $ii_links_bayes =   foreach (join $item_weights by item, $ii_links_raw by item_B) generate
                            item_A as item_A,
                            item_B as item_B,
                            (float) ((weight * $pop_boost_func(overall_weight)) / (overall_weight + $prior))
                            as weight,
                            weight as raw_weight;
};


/*
 * This is an alternative to recsys__BuildItemItemRecommendationsFromGraph.
 *
 * To improve performance this version only finds recommendations for an item from its
 * direct neighbours.
 *
 * Input:
 *      ii_links: { (item_A:chararray, item_B:chararray, weight:float, raw_weight:float) }
 *      num_recs: int
 *
 * Ouptut:
 *      item_recs: { (item_A:chararray, item_B:chararray, weight:float, raw_weight:float, rank:int) }
 */
define recsys__BuildItemItemRecommendationsFromGraph_skipShortestPaths(ii_links, num_recs)
returns item_recs {

    item_recs_full    =   foreach (group $ii_links by item_A) {
                            sorted = order $1 by weight desc;
                               top = limit sorted $num_recs;
                            generate flatten(recsys__Enumerate(top))
                                  as (item_A, item_B, weight, raw_weight, rank);
                          }

    $item_recs     =   foreach item_recs_full generate $0..$3, (int) rank;
};


/*
 * This is an alternative of recsys__BuildItemItemRecommendationsFromGraph.  
 *
 * This takes an additional input of a set of available source items and available destination
 * items, to handle the case where not every item is in stock or needs a recommendation; but the
 * links to those items may still be valuable in the shortest paths traversal and when linking
 * back to users.
 *
 * Input:
 *      Same inputs as recsys__BuildItemItemRecommendationsFromGraph.
 *      source_items: { (item:chararray) }
 *      dest_items: { (item:chararray) }
 *
 * Output:
 *      Same output as recsys__BuildItemItemRecommendationsFromGraph.
 */
define recsys__BuildItemItemRecommendationsFromGraph_withAvailableItems(
                            ii_links, source_items, dest_items, initial_nhood_size, num_recs)
returns item_recs {

    graph, paths        =   recsys__InitShortestPaths_FromAvailableItems($ii_links,
                                                                      $source_items,
                                                                      $dest_items,
                                                                      $initial_nhood_size);

    two_step_terms      =   foreach (join graph by item_B, paths by item_A) generate
                                graph::item_A as item_A,
                                paths::item_B as item_B,
                                graph::dist + paths::dist as dist,
                                (paths::item_A == paths::item_B ?
                                    graph::raw_weight : paths::raw_weight) as raw_weight;

    shortest_paths      =   foreach (group two_step_terms by (item_A, item_B)) generate
                                flatten(recsys_udfs.best_path($1))
                                as (item_A, item_B, dist, raw_weight);
    shortest_paths      =   filter shortest_paths by item_A != item_B;

    -- jython udf returns doubles so recast to float
    shortest_paths      =   foreach shortest_paths generate
                                item_A, item_B, (float) dist, (float) raw_weight;

    nhoods_tmp          =   foreach (group shortest_paths by item_A) {
                                ordered = order $1 by dist asc;
                                    top = limit ordered $num_recs;
                                generate flatten(recsys__Enumerate(top))
                                      as (item_A, item_B, dist, raw_weight, rank);
                            }

    $item_recs          =   foreach nhoods_tmp generate
                                item_A, item_B, 1.0f / dist as weight, raw_weight, (int) rank;
};

/*
 * Helper method for recsys__BuildItemItemRecommendationsFromGraph_withAvailableItems.
 *
 * Construct distance and path graphs for use in the shortest path algorithm.
 * 
 * Input:
 *      ii_links: { (item_A:chararray, item_B:chararray, weight:float, raw_weight:float) }
 *      source_items: { (item:chararray) }
 *      dest_items: { (item:chararray) }
 *      num_recs: int
 *
 * Output:
 *       graph: { (item_A:chararray, item_B:chararray, dist:float, raw_weight:float) }
 *       paths: { (item_A:chararray, item_B:chararray, dist:float, raw_weight:float) }
 */
define recsys__InitShortestPaths_FromAvailableItems(ii_links, source_items, dest_items, num_recs)
returns graph, paths {

    distance_mat        =   foreach $ii_links generate
                                item_A, item_B, 1.0f / weight as dist, raw_weight;

    graph_tmp           =   foreach (group distance_mat by item_A) {
                                sorted = order $1 by dist asc;
                                   top = limit sorted $num_recs;
                                generate flatten(top)
                                      as (item_A, item_B, dist, raw_weight);
                            }

    $graph              =   foreach (join $source_items by item, graph_tmp by item_A) generate
                                item_A as item_A, item_B as item_B,
                                dist as dist, raw_weight as raw_weight;

    graph_copy          =   foreach graph_tmp generate item_A, item_B, dist, null as raw_weight;
    dest_verts_dups     =   foreach graph_copy generate item_B as id;
    dest_verts          =   distinct dest_verts_dups;
    self_loops          =   foreach dest_verts generate
                                id as item_A, id as item_B, 0.0f as dist, null as raw_weight;
    raw_paths           =   union graph_copy, self_loops;
    $paths              =   foreach (join raw_paths by item_B, $dest_items by item) generate
                                raw_paths::item_A as item_A, 
                                raw_paths::dist as dist,
                                raw_paths::raw_weight as raw_weight,
                                raw_paths::item_B as item_B;
};



/*
 * Helper Method for building an item-item graph with additional item-item signals
 * Helper for recsys__GetItemItemRecommendations_AddItemItem 
 *
 * This Macro is used to re-sum the total item weights from item-item links already considered 
 * and item-item links not yet considered.
 *
 * Input:
 *      ii_links_weighted: { (item_A:chararray, item_B:chararray, weight:float) }
 *      ii_links_not_weighted: { (item_A:chararray, item_B:chararray, weight:float) }
 *      item_weights: { (item:chararray, overall_weight:float) }
 * Output:
 *      ii_links_combined: { (item_A:chararray, item_B:chararray, weight:float) }
 *      item_weights_combined: { (item:chararray, overall_weight:float) }
 */
define recsys__SumItemItemSignals(ii_links_weighted, ii_links_not_weighted, item_weights) 
returns ii_links_combined, item_weights_combined {

    -- Sums together the overall weights for newly added item-item signals
    ii_no_weight_summed = foreach (group $ii_links_not_weighted by item_A) generate
                                               group as item,
                              (float) SUM($1.weight) as overall_weight;

    -- joins together the two item item signals in order to pair the corresponding weights together
    item_weights_joined = join $item_weights by item FULL, ii_no_weight_summed by item;

    item_weights_combined_temp = foreach item_weights_joined generate
                                    (item_weights::item is not null ? 
                                        item_weights::item : ii_no_weight_summed::item) 
                                                    as item,
                                    (float) (item_weights::overall_weight + 
                                                ii_no_weight_summed::overall_weight) 
                                                    as overall_weight;
    
    -- if overall_weight is negative, set it to zero
    $item_weights_combined = foreach item_weights_combined_temp generate
                                item,
                                (overall_weight < 0 ? 0.0 : overall_weight) as overall_weight;

    -- we only want positive numbers to prevent a divide by zero later on
    $ii_links_combined = filter (union $ii_links_weighted, $ii_links_not_weighted) by weight > 0;
};

/*
 * Helper Method recsys__GetItemItemRecommendations_DiversifyItemItem 
 * This is used to diversify item-item links.
 *
 * Input:
 *      ii_links: { (item_A:chararray, item_B:chararray, weight:float) }
 *      metadata: { (item:chararray, metadata_field:chararray) }
 * Output:
 *      ii_links_diverse: { (item_A:chararray, metadata_A:chararray, item_B:chararray, 
 *                           metadata_B:chararray, weight:float, raw_weight:float) }
 */
define recsys__DiversifyItemItemLinks (ii_links, metadata) returns ii_links_diverse{

    feature_join      = foreach (join $ii_links by item_B, $metadata by item) generate
                                    item_A as item_A,
                                    item_B as item_B,
                                    weight as weight,
                                raw_weight as raw_weight,
                            metadata_field as metadata_field;

    feature_ranks     = foreach (group feature_join by (item_A, metadata_field)) {
                            sorted = order $1 by weight desc;
                            generate flatten(recsys__Enumerate(sorted))
                                           as (item_A, item_B, weight, raw_weight, 
                                               metadata_field, feature_rank);
                        }

    $ii_links_diverse = foreach feature_ranks generate 
                            item_A, item_B,
                            (float) (weight / feature_rank) as weight,
                            raw_weight;
};

