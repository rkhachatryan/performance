package com.github.projectflink.common;

import com.github.projectflink.common.AnalyzeTool.Result;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class AnalyzeToolTest {
	public static final double EPSILON = 0.001;

	@Test
	public void analyzeLogFile() throws IOException {
		Result result = AnalyzeTool.analyze("src/test/resources/example-log.gz");

		assertEquals(385927.23443877226, result.throughputs.getMean(), EPSILON);
		assertEquals(4545454.545454545, result.throughputs.getMax(), EPSILON);
		assertEquals(55437, result.throughputs.getN());

		assertEquals(3290.572362395614, result.latencies.getMean(), EPSILON);

		assertEquals(10, result.perHostLatancies.size());
		assertEquals(10, result.perHostThroughputs.size());

		assertEquals(4785, result.checkpointDurations.getPercentile(50), EPSILON);
		assertEquals(6220, result.checkpointDurations.getPercentile(99), EPSILON);
	}
}