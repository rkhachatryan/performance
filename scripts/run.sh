#!/bin/bash

RUNNAME=$1
FLINK_BIN=${2:-flink}
JOB_JAR=${3:-flink-jobs-0.1-SNAPSHOT.jar}
ANALYZE_JAR=${4:-perf-common-0.1-SNAPSHOT-jar-with-dependencies.jar}

NODES=${5:-10 20 30}
SLOTS=${6:-4}
BUFFER_TIMEOUTS_THROUGHPUT=${7:-0 5 10}
BUFFER_TIMEOUTS_LATENCY=${8:-0 10 100}
CREDIT_BASED_ENABLED=${9:-true}
CLASS=${10:-com.github.projectflink.streaming.Throughput}

LOG="run-log-${RUNNAME}"

export HADOOP_CONF_DIR=/etc/hadoop/conf
BUFFER_TIMEOUT=100
REPART=1
SOURCE_DELAY=0 # in ms
SOURCE_DELAY_FREQ=0 # every this many records
LATENCY_MEASURE_FREQ=100000 # every this many records
LOG_FREQ=100000 # every this many records
PAYLOAD_SIZE=12
CHECKPOINTING_INTERVAL=""
start_job() {
	nodes=$1
	slots=$2
	echo -n "$nodes;$slots;$REPART;$CHECKPOINTING_INTERVAL;$BUFFER_TIMEOUT;$SOURCE_DELAY;$SOURCE_DELAY_FREQ;$LATENCY_MEASURE_FREQ;$LOG_FREQ;$PAYLOAD_SIZE;$CREDIT_BASED_ENABLED;" >> $LOG
	echo "Starting job on YARN with $nodes workers and a buffer timeout of $BUFFER_TIMEOUT ms (source delay $SOURCE_DELAY)"
	PARA=$(($1*$slots))
	"${FLINK_BIN}" run -m yarn-cluster -yn $1 -yst -yD taskmanager.network.credit-based-flow-control.enabled=$CREDIT_BASED_ENABLED -yjm 768 -ytm 3072 -ys $slots -d -p $PARA -c "$CLASS" "${JOB_JAR}" $CHECKPOINTING_INTERVAL --sleepFreq $SOURCE_DELAY_FREQ --repartitions $REPART --timeout $BUFFER_TIMEOUT --payload $PAYLOAD_SIZE --delay $SOURCE_DELAY --logfreq $LOG_FREQ --latencyFreq $LATENCY_MEASURE_FREQ | tee lastJobOutput
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
	name=$1
	appid=$2
	sleep 30
	yarn logs -applicationId $appid |& gzip -c > logs/${RUNNAME}-${name}-${appid}.gz
}
analyzeLogs() {
	name=$1
	appid=$2
	java -cp "${ANALYZE_JAR}" com.github.projectflink.common.AnalyzeTool logs/${RUNNAME}-${name}-${appid}.gz >> ${LOG}
}

function experiment() {
	name=$1
	nodes=$2
	slots=$3
	dur=$4
	start_job ${nodes} ${slots}
	duration ${dur}
	APPID=`kill_on_yarn`

	getLogsFor ${name} ${APPID}
	analyzeLogs ${name} ${APPID}
}

mkdir -p logs

echo "machines;slots;repartitions;checkpointing_interval;buffer_timeout;source_delay;source_delay_freq;latency_measure_freq;log_freq;payload_size;credit_based;duration-sec;jobId;machines_results;lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs;checkpoint-duration-median;checkpoint-duration-99percentile" >> $LOG

# DURATION=900 # 15min
DURATION=300 # 5min

#redo throughput tests
REPART=1 # only one re-partitioning via a (hashing) key-by
BUFFER_TIMEOUT=5 # default 5ms

for slots in $SLOTS; do
	for nodes in $NODES; do
		experiment throughput1 $nodes $slots $DURATION
		experiment throughput1 $nodes $slots $DURATION
		experiment throughput1 $nodes $slots $DURATION
	done
done

# test throughput and the impact of the timeouts on the throughput and latency

REPART=2

for CHECKPOINTING_INTERVAL in "" " --ft 1000 "; do
	for BUFFER_TIMEOUT in $BUFFER_TIMEOUTS_THROUGHPUT; do
		for slots in $SLOTS; do
			for nodes in $NODES; do
				experiment throughput2 $nodes $slots $DURATION
				experiment throughput2 $nodes $slots $DURATION
				experiment throughput2 $nodes $slots $DURATION
			done
		done
	done
done

# latency benchmark
SOURCE_DELAY=100 # in ms
SOURCE_DELAY_FREQ=1 # every this many records
for BUFFER_TIMEOUT in $BUFFER_TIMEOUTS_LATENCY; do
  for SOURCE_DELAY in 1 5 10 50 100; do
	for slots in $SLOTS; do
		for nodes in $NODES; do
			# roughly once per second
			LOG_FREQ=$(($SOURCE_DELAY_FREQ*1000/$SOURCE_DELAY)) # every this many records
			LATENCY_MEASURE_FREQ=$(($LOG_FREQ/5))
			experiment latency2 $nodes $slots $DURATION
			experiment latency2 $nodes $slots $DURATION
			experiment latency2 $nodes $slots $DURATION
		done
	done
  done
done

exit

