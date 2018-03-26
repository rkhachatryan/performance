package com.github.projectflink.common;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class AnalyzeTool {
	/**
	 * After first detection of NORMAL or STORM mode, skip trying to parse the other one.
	 */
	public enum HostDetectionMode {
		UNKNOWN,
		NORMAL,
		STORM
	}

	public static class Result {

		DescriptiveStatistics latencies;
		ThroughputMean throughputs;
		Map<String, DescriptiveStatistics> perHostLatancies;
		Map<String, ThroughputMean> perHostThroughputs;

		public Result(
				DescriptiveStatistics latencies,
				ThroughputMean throughputs,
				Map<String, DescriptiveStatistics> perHostLatancies,
				Map<String, ThroughputMean> perHostThroughputs) {
			this.latencies = latencies;
			this.throughputs = throughputs;
			this.perHostLatancies = perHostLatancies;
			this.perHostThroughputs = perHostThroughputs;
		}
	}

	public static Result analyze(String file) throws IOException {

		final InputStream is;
		if (file.endsWith(".gz")) {
			is = new GZIPInputStream(new FileInputStream(file));
		} else {
			is = new FileInputStream(file);
		}
		BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));

		String l;
		Pattern latencyPattern = Pattern.compile(".*Latency ([0-9]+) ms.*");
		Pattern throughputPattern = Pattern.compile(".*we received ([0-9]+) elements. That's ([0-9.]+) elements\\/second\\/core.*");
		Pattern hostPattern = Pattern.compile("Container: .* on ([^.]+).*");
		Pattern stormHostPattern = Pattern.compile(".*Client environment:host.name=([^.]+).*");

		DescriptiveStatistics latencies = new DescriptiveStatistics();
		ThroughputMean throughputs = new ThroughputMean();
		String currentHost = null;
		Map<String, DescriptiveStatistics> perHostLat = new HashMap<>();
		Map<String, ThroughputMean> perHostThroughputs = new HashMap<>();
		HostDetectionMode hostDetectionModeMode = HostDetectionMode.UNKNOWN;

		while ((l = br.readLine()) != null) {
			// Due to performance reason, first parse lines that are the most likely to occur.
			// ---------- latency ---------------
			Matcher latencyMatcher = latencyPattern.matcher(l);
			if(latencyMatcher.matches()) {
				int latency = Integer.valueOf(latencyMatcher.group(1));
				latencies.addValue(latency);

				DescriptiveStatistics perHost = perHostLat.get(currentHost);
				if(perHost == null) {
					perHost = new DescriptiveStatistics();
					perHostLat.put(currentHost, perHost);
				}
				perHost.addValue(latency);
				continue;
			}

			// ---------- throughput ---------------
			Matcher tpMatcher = throughputPattern.matcher(l);
			if(tpMatcher.matches()) {
				long elements = Long.valueOf(tpMatcher.group(1));
				double throughput = Double.valueOf(tpMatcher.group(2));
				// since throughputs are being reported once per N records (and not once per time interval), faster tasks
				// will report more throughputs with higher values skewing the average. To fix that, we are decomposing
				// measured throughput into number of elements and time, and we recalculate average after aggregating
				// all measurements
				double time = elements / throughput;
				throughputs.addMeassurement(elements, time);

				ThroughputMean perHost = perHostThroughputs.get(currentHost);
				if(perHost == null) {
					perHost = new ThroughputMean();
					perHostThroughputs.put(currentHost, perHost);
				}
				perHost.addMeassurement(elements, time);
				continue;
			}

			// ---------- host ---------------
			if (hostDetectionModeMode != HostDetectionMode.STORM) {
				Matcher hostMatcher = hostPattern.matcher(l);
				if (hostMatcher.matches()) {
					currentHost = hostMatcher.group(1);
					System.err.println("Setting host to " + currentHost);
					hostDetectionModeMode = HostDetectionMode.NORMAL;
					continue;
				}
			}

			if (hostDetectionModeMode != HostDetectionMode.NORMAL) {
				Matcher stormHostMatcher = stormHostPattern.matcher(l);
				if (stormHostMatcher.matches()) {
					currentHost = stormHostMatcher.group(1);
					System.err.println("Setting host to " + currentHost + " (storm)");
					hostDetectionModeMode = HostDetectionMode.STORM;
					continue;
				}
			}
		}

		return new Result(latencies, throughputs, perHostLat, perHostThroughputs);
	}

	public static void main(String[] args) throws IOException {
		Result r1 = analyze(args[0]);
		DescriptiveStatistics latencies = r1.latencies;
		ThroughputMean throughputs = r1.throughputs;
		// System.out.println("lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs;");
		System.out.println("all-machines;" + latencies.getMean() + ";" + latencies.getPercentile(50) + ";" + latencies.getPercentile(90) + ";" + latencies.getPercentile(95) + ";" + latencies.getPercentile(99)+ ";" + throughputs.getMean() + ";" + throughputs.getMax() + ";" + latencies.getN() + ";" + throughputs.getN());

		System.err.println("================= Latency (" + r1.perHostLatancies.size() + " reports ) =====================");
		List<Map.Entry<String, DescriptiveStatistics>> orderedPerHostLatency = new ArrayList<Map.Entry<String, DescriptiveStatistics>>();

		for(Map.Entry<String, DescriptiveStatistics> entry : r1.perHostLatancies.entrySet()) {
			System.err.println("====== "+entry.getKey()+" (entries: "+entry.getValue().getN()+") =======");
			System.err.println("Mean latency " + entry.getValue().getMean());
			System.err.println("Median latency " + entry.getValue().getPercentile(50));
			orderedPerHostLatency.add(entry);
		}

		System.err.println("================= Throughput ("+r1.perHostThroughputs.size()+" reports ) =====================");
		for(Map.Entry<String, ThroughputMean> entry : r1.perHostThroughputs.entrySet()) {
			System.err.println("====== "+entry.getKey()+" (entries: "+entry.getValue().getN()+")=======");
			System.err.println("Mean throughput " + entry.getValue().getMean());
		}
	}
}
