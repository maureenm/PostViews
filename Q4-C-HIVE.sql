
--START HIVE
hive
use db1;
 
--CREATE TABLE TO STORE MAP REDUCE RESULTS
CREATE TABLE IF NOT EXISTS mr_result(word string, id int, tfidf float) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

--LOAD DATA INTO TABLE
LOAD DATA INPATH 'hdfs://cluster-mycluster-m/mapThreeOutputClean.csv' OVERWRITE INTO TABLE mr_result;

--REFERENCE https://stackoverflow.com/questions/46717465/sql-row-number-partition-by-order-by-ignores-order-statement
SELECT * FROM ( SELECT ROW_NUMBER() OVER(PARTITION BY Id ORDER BY tfidf DESC) AS TfRank, * FROM mr_result) n WHERE TfRank IN (1,2,3,4,5,6,7,8,9,10);
