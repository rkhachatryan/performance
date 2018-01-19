#!/bin/bash

RUNNAME=$1
FLINK_BIN=${2:-flink}
JOB_JAR=${3:-flink-jobs-0.1-SNAPSHOT.jar}
ANALYZE_JAR=${4:-perf-common-0.1-SNAPSHOT-jar-with-dependencies.jar}

NODES=${5:-10 20 30}
BUFFER_TIMEOUTS_THROUGHPUT=${6:-0 5 10}
BUFFER_TIMEOUTS_LATENCY=${7:-0 10 100}
CREDIT_BASED_ENABLED=${8:-true}

LOG="run-log-${RUNNAME}"

export HADOOP_CONF_DIR=/etc/hadoop/conf
CREDIT_BASED_ENABLED=true
BUFFER_TIMEOUT=100
REPART=1
SOURCE_DELAY=0 # in ms
SOURCE_DELAY_FREQ=0 # every this many records
LATENCY_MEASURE_FREQ=100000 # every this many records
LOG_FREQ=100000 # every this many records
PAYLOAD_SIZE=12
CHECKPOINTING_INTERVAL=""
SLOTS=4
start_job() {
	nodes=$1
	echo -n "$nodes;$SLOTS;$REPART;$CHECKPOINTING_INTERVAL;$BUFFER_TIMEOUT;$SOURCE_DELAY;$SOURCE_DELAY_FREQ;$LATENCY_MEASURE_FREQ;$LOG_FREQ;$PAYLOAD_SIZE;" >> $LOG
	echo "Starting job on YARN with $nodes workers and a buffer timeout of $BUFFER_TIMEOUT ms (source delay $SOURCE_DELAY)"
	PARA=$(($1*$SLOTS))
	CLASS="com.github.projectflink.streaming.Throughput"
	"${FLINK_BIN}" run -m yarn-cluster -yn $1 -yst -yD taskmanager.network.credit-based.enabled=$CREDIT_BASED_ENABLED -yjm 768 -ytm 3072 -ys $SLOTS -yd -p $PARA -c $CLASS "${JOB_JAR}" $CHECKPOINTING_INTERVAL --sleepFreq $SOURCE_DELAY_FREQ --repartitions $REPART --timeout $BUFFER_TIMEOUT --payload $PAYLOAD_SIZE --delay $SOURCE_DELAY --logfreq $LOG_FREQ --latencyFreq $LATENCY_MEASURE_FREQ | tee lastJobOutput
}

append() {
	echo -n "$1;" >> $LOG
}

duration() {
	sleep $1
	append "$1"
}

kill_on_yarn() {
	KILL=`cat lastJobOutput | grep "yarn application"`
	# get application id by storing the last string
	for word in $KILL
	do
		AID=$word
	done
	append $AID
	echo $AID
	exec $KILL > /dev/null
}

getLogsFor() {
	sleep 30
	yarn logs -applicationId $1 > logs/${RUNNAME}-$1
}
analyzeLogs() {
	java -cp "${ANALYZE_JAR}" com.github.projectflink.common.AnalyzeTool logs/${RUNNAME}-$1 >> ${LOG}
}

function experiment() {
	name=$1
	nodes=$2
	dur=$3
	start_job ${nodes}
	duration ${dur}
	APPID=`kill_on_yarn`

	getLogsFor ${name}-${APPID}
	analyzeLogs ${name}-${APPID}
}

mkdir -p logs

echo "machines;slots;repartitions;checkpointing_interval;buffer_timeout;source_delay;source_delay_freq;latency_measure_freq;log_freq;payload_size;duration-sec;jobId;machines_results;lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs" >> $LOG

# DURATION=900 # 15min
DURATION=300 # 5min

#redo throughput tests
REPART=1 # only one re-partitioning via a (hashing) key-by
BUFFER_TIMEOUT=5 # default 5ms

for nodes in $NODES; do
	experiment throughput1 $nodes $DURATION
	experiment throughput1 $nodes $DURATION
	experiment throughput1 $nodes $DURATION
done

exit

# test throughput and the impact of the timeouts on the throughput and latency

REPART=2

for CHECKPOINTING_INTERVAL in "" " --ft 1000 "; do
	for BUFFER_TIMEOUT in $BUFFER_TIMEOUTS_THROUGHPUT; do
		for nodes in $NODES; do
			experiment throughput2 $nodes $DURATION
			experiment throughput2 $nodes $DURATION
			experiment throughput2 $nodes $DURATION
		done
	done
done

# latency benchmark
SOURCE_DELAY=100 # in ms
SOURCE_DELAY_FREQ=1 # every this many records
for BUFFER_TIMEOUT in $BUFFER_TIMEOUTS_LATENCY; do
  for SOURCE_DELAY in 1 5 10 50 100; do
	for nodes in $NODES; do
		# roughly once per second
		LOG_FREQ=$(($SOURCE_DELAY_FREQ*1000/$SOURCE_DELAY)) # every this many records
		LATENCY_MEASURE_FREQ=$(($LOG_FREQ/5))
		experiment latency2 $nodes $DURATION
		experiment latency2 $nodes $DURATION
		experiment latency2 $nodes $DURATION
	done
  done
done

exit

