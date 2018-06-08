#!/bin/sh

chkconfig --level 2345 watchdog off
chkconfig --del watchdog
rm -rf /etc/rc.d/init.d/watchdog
