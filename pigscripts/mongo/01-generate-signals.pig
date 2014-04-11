-- If your mongo user has admin privileges you can delete this setting and take advantage of
-- the improvements of splitting your input data.
SET mongo.input.split.create_input_splits false;


/******* Load Data **********/
raw_input =  load '$CONN/$DB.$COLLECTION'
            using com.mongodb.hadoop.pig.MongoLoader('
                      user:chararray,
                      artist_name:chararray,
                      num_plays:int
                  ');

/******* Convert Data to Signals **********/
-- The more times the user plays an artist the stronger the signal.
user_signals = foreach raw_input generate
                 user,
                 artist_name as item,
                 num_plays as weight:int;

rmf $OUTPUT_PATH/user_signals;
store user_signals into '$OUTPUT_PATH/user_signals' using PigStorage();
