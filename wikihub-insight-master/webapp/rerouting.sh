#!/bin/bash
#rerouting.sh
sudo iptables -t nat --flush
sudo iptables -t nat -A PREROUTING -p tcp --dport 80 -j REDIRECT --to-port 5000