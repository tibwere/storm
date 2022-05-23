package org.apache.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TimestampExtractor;
import org.apache.storm.windowing.WaterMarkEventGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value = Parameterized.class)
public class WindowedBoltExecutorTests {

    private boolean isLate;
    private boolean expectedException;
    private Map<String, Object> configurations;
    private WaterMarkEventGenerator waterMarkEventGenerator;
    private WindowedBoltExecutor executor;

    public WindowedBoltExecutorTests(boolean isLate, Map<String, Object> configurations, boolean expectedException) {
        configure(isLate, configurations, expectedException);
    }

    public void configure(boolean isLate, Map<String, Object> configurations, boolean expectedException) {
        this.isLate = isLate;
        this.expectedException = expectedException;
        this.configurations = configurations;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testCasesTuples() {

        Map<String, Object> validConfig = new HashMap<>();
        validConfig.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 1);

        return Arrays.asList(new Object[][]{
                // IS_LATE  CONFIGURATION       EXPECTED_EXCEPTION
                {  true,    validConfig,        false   },
                {  false,   validConfig,        false   },
                {  true,    new HashMap<>(),    true    }
        });
    }

    @Before
    public void setupEnvironment() {
        IWindowedBolt bolt = mock(IWindowedBolt.class);
        TimestampExtractor tsExtractor = mock(TimestampExtractor.class);
        when(bolt.getTimestampExtractor()).thenReturn(tsExtractor);

        this.waterMarkEventGenerator = mock(WaterMarkEventGenerator.class);
        when(this.waterMarkEventGenerator.track(any(), anyLong())).thenReturn(!this.isLate);

        this.executor = new WindowedBoltExecutor(bolt);
    }

    @Test
    public void testAckGenerated() {
        Tuple tuple = mock(Tuple.class);
        TopologyContext ctx = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);

        try {
            executor.prepare(this.configurations, ctx, collector);
            this.executor.waterMarkEventGenerator = this.waterMarkEventGenerator;

            this.executor.execute(tuple);
            int wantedNumberOfInvocation = (this.isLate) ? 1 : 0;
            Mockito.verify(collector, Mockito.times(wantedNumberOfInvocation)).ack(tuple);

            Assert.assertFalse("An \"IllegalArgumentException\" should be thrown", this.expectedException);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue("An exception should not be thrown", this.expectedException);
        }
    }
}
