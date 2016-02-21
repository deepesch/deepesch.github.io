#!/bin/bash

NAME="wikihub"
FLASKDIR=/home/ubuntu/wikihub-insight/webapp
VENVDIR=/home/ubuntu/wikihub-insight/webapp/flenv
SOCKFILE=/home/ubuntu/sock
USER=ubuntu
GROUP=ubuntu
NUM_WORKERS=4

echo "Starting $NAME"

# activate the virtualenv
cd $VENVDIR
source bin/activate

export PYTHONPATH=$FLASKDIR:$PYTHONPATH

# Create the run directory if it doesn't exist
RUNDIR=$(dirname $SOCKFILE)
test -d $RUNDIR || mkdir -p $RUNDIR

# Start your unicorn
exec gunicorn wikihub_worker:app -b 0.0.0.0:80 \
  --name $NAME \
  --workers $NUM_WORKERS \
  --user=$USER --group=$GROUP \
  --log-level=debug \
  --bind=unix:$SOCKFILE

