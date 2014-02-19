/**
 *  This script is an example recommender (using data from http://www.informatik.uni-freiburg.de/~cziegler/BX/)
 *  that demonstrates the add item-item links technique.  The item-item links are generated based on existing
 *  common traits in the set of items. In this case, item-item links are generated based on the author of the
 *  book, where a link is generated when two books have the same author.  
 */
import 'recommenders.pig';



/*
 * Add Item-Item Link Technique
*/
%default INPUT_PATH_USERS '../data/books/users.csv'
%default INPUT_PATH_BOOKS '../data/books/books.csv'
%default INPUT_PATH_RATINGS '../data/books/ratings.csv'
%default OUTPUT_PATH '../data/books/out'
%default OUTPUT_PATH_ADD '$OUTPUT_PATH/add'

/******* Load Data **********/

--Get books signals
book_input =  LOAD '$INPUT_PATH_BOOKS'
                  USING org.apache.pig.piggybank.storage.CSVExcelStorage(';',  'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (
                     isbn:chararray,
                     title:chararray, 
                     author:chararray, 
                     publication_year:int,      
                     publisher:chararray
                  );
user_input =  LOAD '$INPUT_PATH_USERS'
                  USING org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (
                      user_id:int, location:chararray, age:int
                  );

rating_input = LOAD '$INPUT_PATH_RATINGS'
                  USING org.apache.pig.piggybank.storage.CSVExcelStorage(';', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') AS (
                      user_id:int, isbn:chararray, rating:float
                  );
 

/******* Convert Data to Signals **********/

-- fitler to remove ratings of zero
rating_filtered = FILTER rating_input BY rating != 0; 
-- since rating data only gives isbn, replaces it with book title
rating_named = JOIN rating_filtered BY isbn, book_input BY isbn;
-- weight ranges from -0.5 to 0.5, original weighting ranges from 1 to 10 
  -- rating from 1 to 5 results in a negative number as user dislikes item
  -- rating from 6 to 10 results in a positve number as user likes item
user_signals = FOREACH rating_named GENERATE
                  user_id as user,
                  book_input::title as item,
                  ((rating - 5.0) * 0.1) as weight;
                                  

/******* Create Item-Item Links *******/

book_clone = FOREACH book_input GENERATE *;
book_signals = JOIN book_input by author, book_clone by author;
-- only interested in different books, hence filter out books that are the same item
filtered_books = FILTER book_signals BY (book_input::isbn != book_clone::isbn); 
item_signals_raw = FOREACH filtered_books GENERATE
                  book_input::title as item_A,
                  book_clone::title as item_B,
                  0.5               as weight; -- strong reccomendation for similar authors

-- Remove potential erros where item_A and item_B is null
item_signals = FILTER item_signals_raw BY item_A is not null and item_B is not null;



/******* Use Mortar recommendation engine to convert signals to recommendations **********/
-- NOTE uses a non standard macro instead of recsys__GetItemItemRecommendations
-- The item_signals_filt is an extra parameter
item_item_recs_add = recsys__GetItemItemRecommendations_AddItemItem(user_signals, item_signals);

ii_item_filt = FILTER item_item_recs_add BY (item_B is not null) AND (item_A is not null) AND (weight is not null) AND (raw_weight is not null) AND  (rank is not null);
user_item_recs_add = recsys__GetUserItemRecommendations(user_signals, ii_item_filt);



/******* Store recommendations **********/

rmf $OUTPUT_PATH_ADD/item_item_recs;
rmf $OUTPUT_PATH_ADD/user_item_recs;

store item_item_recs_add into '$OUTPUT_PATH_ADD/item_item_recs' using PigStorage();
store user_item_recs_add into '$OUTPUT_PATH_ADD/user_item_recs' using PigStorage();

