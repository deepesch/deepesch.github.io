#!/bin/bash

echo "Checking for existing Wikimedia IRC bot process..."
PROCESS_ID_LIST=( $(ps -ef | grep "wikimedia_ircbot.py" | grep -v grep | awk -F" "  '{print $2;}') )

if [[ -z ${PROCESS_ID_LIST} ]]
then
	echo "Existing process not found!"
else
	for process_id in "${PROCESS_ID_LIST[@]}"
	do
		echo "Found existing process at PID:" ${process_id}
		echo "Killing existing process..."
		kill -s SIGTERM ${process_id}
	done
fi

echo "Launching Wikimedia IRC bot..."
nohup python ./wikimedia_ircbot.py >> ./wikimedia_ircbot.log &

PROCESS_ID_LIST=( $(ps -ef | grep "wikimedia_ircbot.py" | grep -v grep | awk -F" "  '{print $2;}') )
echo "Started Wikimedia IRC bot process at PID:" ${PROCESS_ID_LIST[0]}
