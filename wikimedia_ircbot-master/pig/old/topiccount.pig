A = load '/user/ubuntu/parsedtest.log' using PigStorage('\t') as (timestamp:chararray, topic:chararray, url:chararray, user:chararray, editsize:int, flags:chararray, comment:chararray);
B = foreach A generate flatten(TOKENIZE((chararray)$1)) as topics;
C = group B by topics;
D = foreach C generate COUNT(B), group;
dump D;
