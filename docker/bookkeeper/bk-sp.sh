#!/bin/sh
set -xe

RECOEVER_LOG_PATH=/opt/bookkeeper/logs/recover.log
usage() {
    cat <<EOF
usage: ${0} [OPTIONS]

The following flags are required.
       --type, -t             service procedure type(precheck, recover, postcheck).
       --timeout, -o          timeout for recovery process in seconds.
EOF
    exit 1
}

precheck() {
if curl "http://`hostname -i`:8080/api/v1/autorecovery/list_under_replicated_ledger" | grep -q "No under replicated ledgers found"
then
  echo `date +%Y-%m-%d.%H:%M:%S`" pass precheck" >> $RECOEVER_LOG_PATH
	exit 0
else
	echo `date +%Y-%m-%d.%H:%M:%S`" under_replicated_ledger not empty, precheck failed" >> $RECOEVER_LOG_PATH
	exit 1
fi
}

postcheck() {
recoveryinternal
if curl "http://`hostname -i`:8080/api/v1/autorecovery/list_under_replicated_ledger" | grep -q "No under replicated ledgers found"
then
  echo `date +%Y-%m-%d.%H:%M:%S`" pass postcheck" >> $RECOEVER_LOG_PATH
	exit 0
else
	echo `date +%Y-%m-%d.%H:%M:%S`" under_replicated_ledger not empty, postcheck failed" >> $RECOEVER_LOG_PATH
	exit 1
fi
}


recovery() {
if curl "http://`hostname -i`:8080/api/v1/autorecovery/list_under_replicated_ledger" | grep -q "No under replicated ledgers found"
then
	timeout ${timeout_seconds}s /opt/bookkeeper/bin/bookkeeper shell recover `hostname -i`:3181 -f >> $RECOEVER_LOG_PATH
	if grep "OK: No problem" /opt/bookkeeper/logs/bookkeeper-server.log | grep -q `date +%Y-%m-%d.%H`
	then
	  echo `date +%Y-%m-%d.%H:%M:%S`" recovery succeeded" >> $RECOEVER_LOG_PATH
	  exit 0
	else
		echo `date +%Y-%m-%d.%H:%M:%S`" recovery failed" >> $RECOEVER_LOG_PATH
		exit 1
	fi
else
	echo `date +%Y-%m-%d.%H:%M:%S`" under_replicated_ledger not empty, recovery failed" >> $RECOEVER_LOG_PATH
	exit 1
fi
}

recoveryinternal() {
timeout ${timeout_seconds}s /opt/bookkeeper/bin/bookkeeper shell recover `hostname -i`:3181 -f >> $RECOEVER_LOG_PATH
if grep "OK: No problem" /opt/bookkeeper/logs/bookkeeper-server.log | grep -q `date +%Y-%m-%d.%H`
then
  echo `date +%Y-%m-%d.%H:%M:%S`" recovery succeeded" >> $RECOEVER_LOG_PATH
else
  echo `date +%Y-%m-%d.%H:%M:%S`" recovery failed" >> $RECOEVER_LOG_PATH
  exit 1
fi
}

while [ $# -gt 0 ]; do
    case ${1} in
        -t|--type)
            type="$2"
            shift
            shift
            ;;
        -o|--timeout)
            timeout_seconds="$2"
            shift
            shift
            ;;
        *)
            usage
            shift
            ;;
    esac
done

case ${type} in
    precheck)
        precheck
        ;;
    postcheck)
        postcheck
        ;;
    recover)
        recovery
        ;;
    *)
        usage
        ;;
esac