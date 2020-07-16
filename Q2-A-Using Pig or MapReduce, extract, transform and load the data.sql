--EXECUTE PIG
pig

--LOAD IN ALL FOUR CSV FILES WHICH CONTAIN THE STACK OVERFLOW DATASETS.
--USING CSVExcelStorage TO HANDLE THE LINE BREAKS, COMMAS AND TO REMOVE HEADERS
a = load 'hdfs://cluster-mycluster-m/assigone/top50.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS ( id:int, posttypeid:int, acceptedanswerid:int, parentid:int, creationdate:chararray, deletiondate:chararray, score:int, viewcount:int, body:chararray, owneruserid:int, ownerdisplayname:chararray, lasteditoruserid:int, lasteditordisplayname:chararray, lasteditdate:chararray, lastactivitydate:chararray, title:chararray, tags:chararray, answercount:int, commentcount:int, favoritecount:int, closeddate:chararray, communityowneddate:chararray);
b = load 'hdfs://cluster-mycluster-m/assigone/top50to100.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS ( id:int, posttypeid:int, acceptedanswerid:int, parentid:int, creationdate:chararray, deletiondate:chararray, score:int, viewcount:int, body:chararray, owneruserid:int, ownerdisplayname:chararray, lasteditoruserid:int, lasteditordisplayname:chararray, lasteditdate:chararray, lastactivitydate:chararray, title:chararray, tags:chararray, answercount:int, commentcount:int, favoritecount:int, closeddate:chararray, communityowneddate:chararray);
c = load 'hdfs://cluster-mycluster-m/assigone/top100to150.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS ( id:int, posttypeid:int, acceptedanswerid:int, parentid:int, creationdate:chararray, deletiondate:chararray, score:int, viewcount:int, body:chararray, owneruserid:int, ownerdisplayname:chararray, lasteditoruserid:int, lasteditordisplayname:chararray, lasteditdate:chararray, lastactivitydate:chararray, title:chararray, tags:chararray, answercount:int, commentcount:int, favoritecount:int, closeddate:chararray, communityowneddate:chararray);
d = load 'hdfs://cluster-mycluster-m/assigone/top150to200.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','YES_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') AS ( id:int, posttypeid:int, acceptedanswerid:int, parentid:int, creationdate:chararray, deletiondate:chararray, score:int, viewcount:int, body:chararray, owneruserid:int, ownerdisplayname:chararray, lasteditoruserid:int, lasteditordisplayname:chararray, lasteditdate:chararray, lastactivitydate:chararray, title:chararray, tags:chararray, answercount:int, commentcount:int, favoritecount:int, closeddate:chararray, communityowneddate:chararray);

--UNIONING ALL CSV files
un = UNION a,b,c,d;

--REMOVING THE LINE BREAKS AND TAGS
--REMOVING UNWANTED COLUMNS BY ONLY GENERATING THE FOUR REQUIRED FOR THE ASSIGNMENT QUESTIONS
clean_data = FOREACH un GENERATE id, score, owneruserid, REPLACE(REPLACE(REPLACE(body, '\n*', ''),',*',''),'<.*?>','') AS body;

--REMOVING NULLS FROM DATA
remove_null = FILTER clean_data BY (owneruserid IS NOT NULL) AND (score IS NOT NULL);

--STORING THE CLEAN DATASET IN HDFS
--STORING WITH PIPE AS DELIMITER SO FILE CAN BE LOADED INTO HIVE EASILY
store remove_null INTO 'hdfs://cluster-mycluster-m/clean_data' USING org.apache.pig.piggybank.storage.CSVExcelStorage('|','NO_MULTILINE','NOCHANGE','SKIP_OUTPUT_HEADER');


--QUIT PIG
quit

--CONFIRM YOUR FILE IS IN HDFS
hadoop fs -ls '/clean_data'




