#!/bin/bash
# take a directory of files and create/populate kafka topics based on file name

source ./kafka.config

SOURCE_LOG_PATH=$1
LOG_EXTENSION=$2


# input argument handling
if [[ -z ${SOURCE_LOG_PATH} ]]
then
	echo "No source log path specified. Using default path: '../logs'"
	SOURCE_LOG_PATH="../logs"
fi

if [[ -z ${LOG_EXTENSION} ]]
then
	echo "No log file extension specified. Using default extension: .log" 
	LOG_EXTENSION=".log"
fi


# get a list of log files to be fed into kafka topics
LOG_FILE_LIST=(${SOURCE_LOG_PATH}/*${LOG_EXTENSION})

echo ${#LOG_FILE_LIST[@]} log files found


# create and populate kafka topics using existing log files
# we assume the kafka topics already exist or the kafka broker is configure to auto create
# a topic will be created/populated for each log file found, respectively
for log in ${LOG_FILE_LIST[@]}
do
	echo ${log}
	
	# strip log file extension
	logfile=( $(basename ${log%${LOG_EXTENSION}}) )

	# Future: strip datestamps from log file names
	# Consider making this an input source requirement

	# Valid topic names include alphanumeric, dash, and underscore, max length of 255
	# Refer to KAFKA-495: https://issues.apache.org/jira/browse/KAFKA-495

	# Input cleaning for topic name
	topicname=${logfile//[!a-z0-9-_.]}

	# pipe log contents to Kafka topic
	cat ${log} | ${KAFKA_BIN_PATH}/kafka-console-producer.sh \
		--broker-list ${KAFKA_BROKER} --topic ${topicname}
done
