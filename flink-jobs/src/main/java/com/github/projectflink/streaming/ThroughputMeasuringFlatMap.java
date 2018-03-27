package com.github.projectflink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import com.github.projectflink.streaming.Throughput.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

class ThroughputMeasuringFlatMap implements FlatMapFunction<Type, Integer> {
	private static final Logger LOG = LoggerFactory.getLogger(ThroughputMeasuringFlatMap.class);

	private static final long serialVersionUID = -4881110695631095859L;

	private final int bytesPerMessage;
	private final long logfreq;

	private Integer host;
	private long received;
	private long start;
	private long lastLog = -1;
	private long lastElements;

	public ThroughputMeasuringFlatMap(int bytesPerMessage, long logfreq) {
		this.bytesPerMessage = bytesPerMessage;
		this.logfreq = logfreq;
	}

	@Override
	public void flatMap(Throughput.Type element, Collector<Integer> collector) throws Exception {
		if (host == null) {
			host = Throughput.convertHostnameToInt(InetAddress.getLocalHost().getHostName());
		}
		if (start == 0) {
			start = System.currentTimeMillis();
		}

		received++;
		if (received % logfreq == 0) {
			// throughput over entire time
			long now = System.currentTimeMillis();
			long sinceSec = ((now - start) / 1000);
			if (sinceSec == 0) return;
			LOG.info("Received {} elements since {}. Elements per second {}, GB received {}",
					received,
					sinceSec,
					received / sinceSec,
					(received * bytesPerMessage) / 1024 / 1024 / 1024);

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
		if (element.f2 != 0 /* && element.f1.equals(host) */) {
			long lat = System.currentTimeMillis() - element.f2;
			LOG.info("Latency {} ms from machine " + element.f1, lat);
		}
	}
}
