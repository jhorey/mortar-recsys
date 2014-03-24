
/*
 * This macro returns only the user signals that helped form a recommendation between two items.
 *
 * Input:
 *      item1: chararray
 *      item2: chararray
 *      user_signals: { (user:chararray, item:chararray, signal:chararray) }
 *
 * Output:
 *      ordered_links: { (user:chararray, item:chararray, signal:chararray)}
 */
define recsys__SelectSignals(item1, item2, user_signals)
    returns ordered_links {

    user_signals_copy = foreach $user_signals generate *;
    user_linked = join $user_signals by user, user_signals_copy by user;
    filtered_links = filter user_linked by $user_signals::item == '$item1' and user_signals_copy::item == '$item2';
    users = foreach filtered_links generate $user_signals::user as user_keep;
    users = distinct users;

    filtered_signals = join $user_signals by user, users by user_keep;
    keep_signals = foreach filtered_signals generate user, item, signal;
    $ordered_links = ORDER keep_signals by user ASC, item ASC, signal ASC;

};