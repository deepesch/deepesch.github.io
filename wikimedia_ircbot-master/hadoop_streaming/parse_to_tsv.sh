
zcat merged.log.gz | python parselog_tsv.py > merged.tsv

hdfs dfs -copyFromLocal -f merged.tsv /user/ubuntu/enwikipedia_logs
