package com.github.projectflink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Latency {
	private static final Logger LOG = LoggerFactory.getLogger(Latency.class);

	static final int DEFAULT_PAYLOAD_SIZE = 12;
	static final int DEFAULT_DELAY = 0;
	static final int DEFAULT_LATENCY_FREQUENCY = 1_000_000;
	static final int DEFAULT_LOG_FREQUENCY = 1000;

	public static class Type extends Tuple5<Long, String, Long, byte[], ArrayList<String>> {
		private static final long serialVersionUID = 8384943526732634125L;

		public Type() {
		}

		public Type(Long value0, String value1, Long value2, byte[] value3, ArrayList<String> v4) {
			super(value0, value1, value2, value3, v4);
		}
	}

	public static class Source extends RichParallelSourceFunction<Type> implements ListCheckpointed<Long> {
		private static final long serialVersionUID = 2915726091221740739L;

		final ParameterTool pt;
		byte[] payload;
		long id = 0;
		boolean running = true;
		long time = 0;

		public Source(ParameterTool pt) {
			this.pt = pt;
			payload = new byte[pt.getInt("payload", DEFAULT_PAYLOAD_SIZE)];
		}

		@Override
		public void run(SourceContext<Type> sourceContext) throws Exception {
			int delay = pt.getInt("delay", DEFAULT_DELAY);
			int latFreq = pt.getInt("latencyFreq", DEFAULT_LATENCY_FREQUENCY);
			int nextlat = 1000;
			String host = InetAddress.getLocalHost().getHostName();
			LOG.info("Starting data source on host "+host);
			while(running) {
				if (delay > 0) {
					try {
						Thread.sleep(delay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				// move the ID for the latency so that we distribute it among the machines.
				if(id % latFreq == nextlat) {
					time = System.currentTimeMillis();
				//	LOG.info("Sending latency "+time+" from host "+host+" with id "+id);
					if(--nextlat <= 0) {
						nextlat = 1000;
					}
				}
				ArrayList<String> hosts = new ArrayList<String>(3);
				hosts.add(host);
				sourceContext.collect(new Type(id++, host, time, payload, hosts));
				time = 0;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public List<Long> snapshotState(long l, long l1) throws Exception {
			return Collections.singletonList(id);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			this.id = state.isEmpty() ? 0 : state.get(0);
		}
	}

	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().setGlobalJobParameters(pt);

		if(pt.has("timeout")) {
			see.setBufferTimeout(pt.getLong("timeout"));
		}

		if(pt.has("ft")) {
			see.enableCheckpointing(pt.getLong("ft"));
		}

		DataStreamSource<Type> in = see.addSource(new Source(pt));
		DataStream<Type> part = in//.partitionByHash(0);
		.rebalance();
		part.map(new MapFunction<Type, Type>() {
			private static final long serialVersionUID = 1796784967031892115L;

			public String host = null;

			@Override
			public Type map(Type longIntegerLongTuple4) throws Exception {
				if (host == null) {
					host = InetAddress.getLocalHost().getHostName();
				}
				longIntegerLongTuple4.f4.add(host);
				//longIntegerLongTuple4.f0++;
				return longIntegerLongTuple4;
			}
		}).rebalance().map(new MapFunction<Type, Type>() {
			private static final long serialVersionUID = 1726686944574451242L;

			public String host = null;

			@Override
			public Type map(Type longIntegerLongTuple4) throws Exception {
				if (host == null) {
					host = InetAddress.getLocalHost().getHostName();
				}
				longIntegerLongTuple4.f4.add(host);
			//	longIntegerLongTuple4.f0++;
				return longIntegerLongTuple4;
			}
		}).rebalance().flatMap(new FlatMapFunction<Type, Integer>() {
			private static final long serialVersionUID = 1445056605150936004L;

			public String host = null;
			long received = 0;
			long start = 0;
			long logfreq = pt.getInt("logfreq", DEFAULT_LOG_FREQUENCY);
			long lastLog = -1;
			long lastElements = 0;

			@Override
			public void flatMap(Type element, Collector<Integer> collector) throws Exception {
				if (host == null) {
					host = InetAddress.getLocalHost().getHostName();
				}
				if (start == 0) {
					start = System.currentTimeMillis();
				}
				received++;
				if (received % logfreq == 0) {
					// throughput over entire time
					long now = System.currentTimeMillis();

					// throughput for the last "logfreq" elements
					if (lastLog == -1) {
						// init (the first)
						lastLog = now;
						lastElements = received;
					} else {
						long timeDiff = now - lastLog;
						long elementDiff = received - lastElements;
						double ex = (1000 / (double) timeDiff);
						LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core", timeDiff, elementDiff, elementDiff * ex);
						// reinit
						lastLog = now;
						lastElements = received;
					}
				}
				if (element.f2 != 0 && element.f1.equals(host)) {
					long lat = System.currentTimeMillis() - element.f2;
					element.f4.add(host);
					LOG.info("Latency " + lat + " ms from machine " + element.f1 + " on host " + host + " with element " + element.f0 + " hosts: " + element.f4);
				}
			}
		});

		see.execute();
	}
}
