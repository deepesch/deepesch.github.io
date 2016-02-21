#!/bin/bash

source ./kafka.config

TOPIC=$1

if [[ -z ${TOPIC} ]]
then
	echo "No topic specified. Exiting."
	exit
else
	${KAFKA_BIN_PATH}/kafka-simple-consumer-shell.sh --broker-list ${KAFKA_BROKER} --topic ${TOPIC} --partition 0 --print-offsets --skip-message-on-error
fi
