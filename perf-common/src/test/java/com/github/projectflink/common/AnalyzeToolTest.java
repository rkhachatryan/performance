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

		assertEquals(1111726.308427559, result.throughputs.getMean(), EPSILON);
		assertEquals(7142857.142857143, result.throughputs.getMax(), EPSILON);
		assertEquals(23653, result.throughputs.getN());

		assertEquals(10585.502395920254, result.latencies.getMean(), EPSILON);

		assertEquals(4, result.perHostLatancies.size());
		assertEquals(4, result.perHostThroughputs.size());
	}
}