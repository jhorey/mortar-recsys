
----------------------------------------------------------------------------------------------------
-- Two debug macros for returning recs from item ids to string ids/names
-- and also sorting by user/item asc, rank asc
-- so you can inspect the output quickly with the unix "look" command

/*
 * item_nhoods: { item_A: chararray, item_B: chararray, weight: float, raw_weight: float, rank: int }
 * item_names:  { id: chararray, name: chararray }
 * -->
 * with_names:  { item_A: chararray, item_A_name:chararray, item_B: chararray, item_B_name: chararray, weight: float, raw_weight: float, rank: int }
 */
define Recsys__ItemRecNamesFromIds(item_item_recs, item_names) returns debug {
    join_1      =   foreach (join $item_names by $0, $item_item_recs by item_B) generate
                        item_A as item_A, item_B as item_B, $1 as item_B_name,
                        weight as weight, raw_weight as raw_weight, rank as rank;
    with_names  =   foreach (join $item_names by $0, join_1 by item_A) generate
                        item_A as item_A, $1 as item_A_name, item_B as item_B, item_B_name as item_B_name,
                        weight as weight, raw_weight as raw_weight, rank as rank;
    $debug      =   order with_names by item_A asc, rank asc;
};

/*
 * user_nhoods: { user: chararray, item: chararray, weight: float,
 *                reason_item: chararray, user_reason_item_weight: float, item_reason_item_weight: float, rank: int }
 * user_names:  { id: chararray, name: chararray }
 * item_names:  { id: chararray, name: chararray }
 * -->
 * debug:       { user: chararray, user_name: chararray, item: chararray, item_name: chararray, score: float,
 *                reason_item: chararray, reason_item_name: chararray, user_reason_item_weight: float, item_reason_item_weight: float, rank: int }
 */
define Recsys__UserRecNamesFromIds(user_item_recs, user_names, item_names)
returns debug {
    join_1      =   foreach (join $item_names by $0 right outer, $user_item_recs by reason_item) generate
                        user as user, $user_item_recs::item as item, weight as weight,
                        reason_item as reason_item, ($1 is not null ? $1 : reason_item) as reason_item_name,
                        user_reason_item_weight as user_reason_item_weight, item_reason_item_weight as item_reason_item_weight,
                        rank as rank;

    join_2      =   foreach (join $item_names by $0, join_1 by item) generate
                        user as user, item as item, $1 as item_name, weight as weight,
                        reason_item as reason_item, reason_item_name as reason_item_name,
                        user_reason_item_weight as user_reason_item_weight, item_reason_item_weight as item_reason_item_weight,
                        rank as rank;

    with_names  =   foreach (join $user_names by $0, join_2 by user) generate
                        user as user, $1 as user_name, item as item, item_name as item_name,
                        weight as weight, reason_item as reason_item, reason_item_name as reason_item_name,
                        user_reason_item_weight as user_reason_item_weight, item_reason_item_weight as item_reason_item_weight,
                        rank as rank;

    $debug      =   order with_names by user asc, rank asc;
};