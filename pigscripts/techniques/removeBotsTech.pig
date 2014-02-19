-- run with: mortar local:illustrate pigscripts/techniques/removeBotsTech.pig -f params/techniques.params
/**
 *  This script is an example recommender (using data from http://www.informatik.uni-freiburg.de/~cziegler/BX/)
 *  that demonstrates the remove bots technique.  Bots are users that have an abnormally high amount of
 *  generated signals such that it ultimately disrupts user-item signals in your set of data.  
 */
import 'recommenders.pig';

/********** Remove Bots Technique ******/
%default INPUT_PATH_USERS '../data/books/users.csv'
%default INPUT_PATH_BOOKS '../data/books/books.csv'
%default INPUT_PATH_RATINGS '../data/books/ratings.csv'
%default OUTPUT_PATH '../data/books/out'
%default OUTPUT_PATH_REMOVE_BOTS'$OUTPUT_PATH/remove_bots'

/******* Load Data **********/

--Get book signals
book_input =  LOAD '$INPUT_PATH_BOOKS'
                  USING org.apache.pig.piggybank.storage.CSVExcelStorage(';') AS (
                     isbn:chararray,
                     title:chararray, 
                     author:chararray, 
                     publication_year:int,      
                     publisher:chararray
                  );
user_input =  LOAD '$INPUT_PATH_USERS'
                  USING org.apache.pig.piggybank.storage.CSVExcelStorage(';') AS (
                      user_id:int, location:chararray, age:int
                  );

rating_input = LOAD '$INPUT_PATH_RATINGS'
                  USING org.apache.pig.piggybank.storage.CSVExcelStorage(';') AS (
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
user_signals_with_bots = FOREACH rating_named GENERATE
                  user_id as user,
                  book_input::title as item,
                  ((rating - 5.0) * 0.1) as weight;
 

/********* Remove user-item signals that are considered bots **********/

-- Threshold is set in param file
user_signals = recsys__RemoveBots(user_signals_with_bots, $THRESHOLD);



/******* Use Mortar recommendation engine to convert signals to recommendations **********/

item_item_recs_remove= recsys__GetItemItemRecommendations(user_signals);
user_item_recs_remove = recsys__GetUserItemRecommendations(user_signals, item_item_recs_remove);



/******* Store recommendations **********/

--dump user_item_recs_II_filt;
rmf $OUTPUT_PATH_REMOVE_BOTS/item_item_recs;
rmf $OUTPUT_PATH_REMOVE_BOTS/user_item_recs;

store item_item_recs_remove into '$OUTPUT_PATH_REMOVE_BOTS/item_item_recs' using PigStorage();
store user_item_recs_remove into '$OUTPUT_PATH_REMOVE_BOTS/user_item_recs' using PigStorage();

