import 'recommenders.pig';
import 'recsys_helper.pig';

/**
 *  Generates artist recommendations based off of last.fm data provided by
 *  http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-360K.html
 *
 *  To run this script you will need to provide a parameter DATABASE_PASS with your database
 *  password.  To set in mortar do:
 *
 *      mortar config:set DATABASE_PASS=<password>
 *
 *  You will also need to have created tables in your target database.
 *
 *  CREATE TABLE <ii-table-name> (from_id CHARACTER VARYING NOT NULL, to_id CHARACTER VARYING,
 *  weight NUMERIC, raw_weight NUMERIC, rank INTEGER NOT NULL, PRIMARY KEY (from_id, rank));
 *
 *  CREATE TABLE <ui-table-name> (from_id CHARACTER VARYING NOT NULL, to_id CHARACTER VARYING,
 *  weight NUMERIC, reason_item CHARACTER VARYING, user_reason_item_weight NUMERIC,
 *  item_reason_item_weight NUMERIC, rank INTEGER NOT NULL, PRIMARY KEY (from_id, rank));
 *
*/

%default INPUT_SIGNALS 's3://mortar-example-data/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv'

%default DATABASE_TYPE 'postgresql'
%default DATABASE_DRIVER 'org.postgresql.Driver'
%default DATABASE_HOST '<host>:<port>'
%default DATABASE_NAME '<dbname>'
%default DATABASE_USER '<username>'
%default II_TABLE '<ii-table-name>'
%default UI_TABLE '<ui-table-name>'

input_signals  =    load '$INPUT_SIGNALS' using PigStorage()
                        as (user: chararray, item_id: chararray, item: chararray, weight: float);

item_item_recs =    recsys__GetItemItemRecommendations(input_signals);
user_item_recs =    recsys__GetUserItemRecommendations(input_signals, item_item_recs);

/******* Store recommendations to your database **********/

store item_item_recs  into 'hdfs:///unused-ignore'
   USING org.apache.pig.piggybank.storage.DBStorage('$DATABASE_DRIVER',
   'jdbc:$DATABASE_TYPE://$DATABASE_HOST/$DATABASE_NAME',
   '$DATABASE_USER',
   '$DATABASE_PASS',
   'INSERT INTO $II_TABLE(from_id,to_id,weight,raw_weight,rank) VALUES (?,?,?,?,?)');

store user_item_recs  into 'hdfs:///unused-ignore'
   USING org.apache.pig.piggybank.storage.DBStorage('$DATABASE_DRIVER',
   'jdbc:$DATABASE_TYPE://$DATABASE_HOST/$DATABASE_NAME',
   '$DATABASE_USER',
   '$DATABASE_PASS',
   'INSERT INTO $UI_TABLE(from_id,to_id,weight,reason_item,user_reason_item_weight,item_reason_item_weight,rank) VALUES (?,?,?,?,?,?,?)');

