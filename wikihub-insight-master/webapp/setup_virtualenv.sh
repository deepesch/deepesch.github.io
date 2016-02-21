#!/bin/bash
# Required for matplotlib
sudo apt-get install libfreetype6-dev
virtualenv flenv
flenv/bin/pip install -r requirements.txt