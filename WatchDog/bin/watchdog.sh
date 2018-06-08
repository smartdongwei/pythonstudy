#!/bin/bash
#chkconfig: - 60 50
# description: watchdog server script
#
# processname: watchdog
# Source function library.
. /etc/rc.d/init.d/functions

# Source networking configuration.
. /etc/sysconfig/network

cd /opt/sanhui/watchdog/

# Check that networking is up.
[ ${NETWORKING} = "no" ] && exit 0
RETVAL=0

# program process name
progname="watchdog"

# program path
prog="/opt/sanhui/watchdog/watchdog"

# kill wait time (S)
waittime="30"

if [ $UID -ne 0 ];
then
echo "$0 $@ : You Must Be Root！"
exit 1
fi
start() {
	# Start daemons.
	echo -n $"Starting $prog: "
	if [ -n "`/sbin/pidof -o %PPID $prog`" ]; then
		echo -n $"$prog: already running"
	failure
		echo
		return 1
	fi
	
	daemon $prog >/dev/null 2>&1 &
	RETVAL=$?
	echo
	[ $RETVAL -eq 0 ] && touch /var/lock/subsys/$progname
	return $RETVAL
}

stop() {
# Stop daemons.
echo -n $"Shutting down $prog: "
if [ -z "`/sbin/pidof -o %PPID $prog`" ]; then
echo -n $"$prog: already stopped"
           rm -f /var/lock/subsys/$progname
           failure
           echo
           return 1
        fi
        killproc $prog
	killproc -d $waittime $prog
        RETVAL=$?
        echo
        [ $RETVAL -eq 0 ] && rm -f /var/lock/subsys/$progname
        return $RETVAL
}
# See how we were called.
case "$1" in
  start)
        start
        ;;
  stop)
        stop
        ;;
  restart|reload)
        stop
        start
        RETVAL=$?
        ;;
  condrestart)
        if [ -f /var/lock/subsys/$prog ]; then
            stop
            start
            RETVAL=$?
        fi
        ;;
  status)
        status $prog
        RETVAL=$?
        ;;
  *)
        echo $"Usage: $0 {start|stop|restart|condrestart|status}"
        exit 1
esac
exit $RETVAL
