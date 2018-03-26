package com.github.projectflink.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ThroughputMeanTest {
	public static final double EPSILON = 0.001;

	@Test
	public void testMean() {
		ThroughputMean mean = new ThroughputMean();
		// reports being generated every 10 records
		// two reporters, one with throughput 30 records/second ...
		mean.addMeassurement(10, 1.0 / 3);
		mean.addMeassurement(10, 1.0 / 3);
		mean.addMeassurement(10, 1.0 / 3);
		// ... another 10 records/second
		mean.addMeassurement(10, 1.0);

		assertEquals(20, mean.getMean(), EPSILON);
		assertEquals(4, mean.getN());
		assertEquals(30, mean.getMax(), EPSILON);
	}
}