#!/bin/sh

chkconfig --level 2345 watchdog off
chkconfig --del watchdog

rm -rf /etc/rc.d/init.d/watchdog
cp -f ./watchdog.sh /etc/rc.d/init.d/watchdog
chmod 777 /etc/rc.d/init.d/watchdog

chkconfig --add watchdog
chkconfig --level 2345 watchdog on
