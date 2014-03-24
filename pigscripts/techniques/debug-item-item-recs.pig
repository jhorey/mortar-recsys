import 'debug.pig';

%default INPUT_PATH_PURCHASES '../data/retail/purchases.json'
%default INPUT_PATH_WISHLIST '../data/retail/wishlists.json'
%default OUTPUT_PATH '../data/retail/out'


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

-- Designate the signal type instead of a weight.
purchase_signals = foreach purchase_input generate
                        user_id    as user,
                        movie_name as item,
                        'PURCHASE' as signal;

wishlist_signals = foreach wishlist_input generate
                        user_id    as user,
                        movie_name as item,
                        'WISHLIST' as signal;

user_signals = union purchase_signals, wishlist_signals;

--Find the signals that linked two items
final_output = recsys__SelectSignals('notting hill', 'wuthering heights', user_signals);

--  If your output folder exists already, hadoop will refuse to write data to it.
rmf $OUTPUT_PATH/signals;
store final_output into '$OUTPUT_PATH/signals' using PigStorage();
