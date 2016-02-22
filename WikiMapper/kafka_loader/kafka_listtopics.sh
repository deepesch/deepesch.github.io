#!/bin/bash

source ./kafka.config

TOPIC_LIST=( $(${KAFKA_BIN_PATH}/kafka-topics.sh \
	--zookeeper ${ZOOKEEPER_SERVER}\
	--list topics) )

if [[ -z ${TOPIC_LIST} ]]
then
	echo "[ ${KAFKA_BROKER} ] - No kafka topics found."
	exit
else
	echo "${#TOPIC_LIST[@]} kafka topics found."
fi

for topic in "${TOPIC_LIST[@]}"
do
	echo "[ ${KAFKA_BROKER} ] - ${topic}"
done
