REGISTER /usr/lib/pig/piggybank.jar; 

a = LOAD '/data/path/' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','NOCHANGE','SKIP_INPUT_HEADER') 
AS ( uniqueId:int, callingNum:chararray, calledNum:chararray, callStartTime:chararray, callDuration:int, dispostion:chararray, callType:chararray, channel:chararray, dstChannel:chararray, latitude:chararray, longitude:chararray );
b = foreach a generate callingNum, callDuration;
c = Group b BY callingNum;
d = foreach c generate group, SUM(b.callDuration);
STORE d INTO 'hdfs PATH' USING PigStorage(',');
