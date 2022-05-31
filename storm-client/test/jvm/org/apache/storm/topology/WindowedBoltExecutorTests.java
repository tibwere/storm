package org.apache.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Test cases for WindowedBoltExecutor class
 *
 * @author Simone Tiberi
 */
@RunWith(value = Enclosed.class)
public class WindowedBoltExecutorTests {

    public static WindowManager<Tuple> getPreparedWindowManager(Map<String, Object> configurations,
                                                                boolean needExtractor,
                                                                boolean stateful) {
        TopologyContext ctx = mock(TopologyContext.class);
        OutputCollector coll = mock(OutputCollector.class);
        IWindowedBolt bolt = mock(IWindowedBolt.class);
        if (needExtractor) {
            TimestampExtractor ex = mock(TimestampExtractor.class);
            when(bolt.getTimestampExtractor()).thenReturn(ex);
        }

        WindowedBoltExecutor windowedBoltExecutor = new WindowedBoltExecutor(bolt);
        if (stateful)
            windowedBoltExecutor.doPrepare(configurations, ctx, coll, new ConcurrentLinkedQueue<>(), true);
        else
            windowedBoltExecutor.prepare(configurations, ctx, coll);

        return windowedBoltExecutor.getWindowManager();
    }

    public static WindowManager<Tuple> getPreparedAndExecutedWM(boolean needExtractor, WaterMarkEventGenerator waterMarkEventGenerator) {
        TopologyContext ctx = mock(TopologyContext.class);
        OutputCollector coll = mock(OutputCollector.class);
        IWindowedBolt bolt = mock(IWindowedBolt.class);
        if (needExtractor) {
            TimestampExtractor ex = mock(TimestampExtractor.class);
            when(bolt.getTimestampExtractor()).thenReturn(ex);
        }

        Map<String, Object> simpleConfig = new HashMap<>();
        simpleConfig.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 1);

        WindowedBoltExecutor windowedBoltExecutor = new WindowedBoltExecutor(bolt);
        windowedBoltExecutor.prepare(simpleConfig, ctx, coll);

        if (waterMarkEventGenerator != null)
            windowedBoltExecutor.waterMarkEventGenerator = waterMarkEventGenerator;

        windowedBoltExecutor.execute(mock(Tuple.class));

        return windowedBoltExecutor.getWindowManager();
    }

    @RunWith(Parameterized.class)
    public static class LatenessTests {
        private boolean isLate;
        private boolean expectedException;
        private Map<String, Object> configurations;
        private WaterMarkEventGenerator waterMarkEventGenerator;
        private WindowedBoltExecutor executor;
        private IWindowedBolt mockedDelegatedBolt;

        public LatenessTests(boolean isLate, Map<String, Object> configurations, boolean expectedException) {
            configure(isLate, configurations, expectedException);
        }

        public void configure(boolean isLate, Map<String, Object> configurations, boolean expectedException) {
            this.isLate = isLate;
            this.expectedException = expectedException;
            this.configurations = configurations;
        }

        /**
         * BOUNDARY VALUE ANALYSIS
         *  - isLate:               [TRUE, FALSE]
         *  - configuration:        [VALID, EMPTY, NULL]
         *  - expectedException:    [TRUE, FALSE]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {

            Map<String, Object> validConfig = new HashMap<>();
            validConfig.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 1);

            Map<String, Object> ltsSpecified = new HashMap<>();
            ltsSpecified.put(Config.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS, 10);
            ltsSpecified.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 1);
            ltsSpecified.put(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, "test-stream");

            Map<String, Object> invalidMap = new HashMap<>();
            invalidMap.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
            invalidMap.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 10000);
            invalidMap.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, 5000);


            return Arrays.asList(new Object[][]{
                    // IS_LATE  CONFIGURATION       EXPECTED_EXCEPTION
                    {  true,    validConfig,        false   },
                    {  false,   validConfig,        false   },
                    {  true,    new HashMap<>(),    true    },
                    {  true,    null,               true    },
                    {  true,    ltsSpecified,       false   },
                    {  false,   invalidMap,         true    }
            });
        }

        /**
         * Setup mocks for decorated instance of IWindowedBolt,
         * Timestamp extractor and WaterMarkEventGenerator.
         *
         * in this phase the lateness of the tuple is set in a mocked way
         */
        @Before
        public void setupEnvironment() {
            this.mockedDelegatedBolt = mock(IWindowedBolt.class);
            TimestampExtractor tsExtractor = mock(TimestampExtractor.class);
            when(mockedDelegatedBolt.getTimestampExtractor()).thenReturn(tsExtractor);

            this.waterMarkEventGenerator = mock(WaterMarkEventGenerator.class);
            when(this.waterMarkEventGenerator.track(any(), anyLong())).thenReturn(!this.isLate);

            this.executor = new WindowedBoltExecutor(this.mockedDelegatedBolt);
        }

        @Test
        public void testAckGenerated() {
            Tuple tuple = mock(Tuple.class);
            TopologyContext ctx = mock(TopologyContext.class);
            OutputCollector collector = mock(OutputCollector.class);

            if (this.configurations != null && this.configurations.containsKey(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM)) {
                Set<String> streams = new HashSet<>();
                streams.add((String) this.configurations.get(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM));
                when(ctx.getThisStreams()).thenReturn(streams);
            }

            try {
                executor.prepare(this.configurations, ctx, collector);
                verify(this.mockedDelegatedBolt, times(1)).prepare(
                        eq(this.configurations), eq(ctx), any());
                this.executor.waterMarkEventGenerator = this.waterMarkEventGenerator;

                this.executor.execute(tuple);
                int wantedNumberOfInvocation = (this.isLate) ? 1 : 0;

                if (this.configurations.containsKey(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM)) {
                    String stream = (String) this.configurations.get(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
                    verify(collector, times(1)).emit(stream, Arrays.asList(tuple), new Values(tuple));
                }

                verify(collector, times(wantedNumberOfInvocation)).ack(tuple);
                Assert.assertFalse("An exception should be thrown", this.expectedException);
            } catch (IllegalArgumentException | NullPointerException e) {
                Assert.assertTrue("An exception should not be thrown instead " +
                                e.getClass().getName() + " -> " + e.getMessage(),
                        this.expectedException);
            }
        }
    }

    public static class TestLateTupleStream {
        @Test
        public void testWrongInitialization() {
            IWindowedBolt bolt = mock(IWindowedBolt.class);
            TopologyContext ctx = mock(TopologyContext.class);
            OutputCollector collector = mock(OutputCollector.class);
            TimestampExtractor ex = mock(TimestampExtractor.class);
            when(bolt.getTimestampExtractor()).thenReturn(ex);

            Map<String, Object> config = new HashMap<>();
            config.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 1);
            config.put(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, "test-stream");

            WindowedBoltExecutor executor = new WindowedBoltExecutor(bolt);
            try {
                executor.prepare(config, ctx, collector);
                Assert.fail("This configuration should be wrong");
            }catch (IllegalArgumentException e) {
                Assert.assertTrue(true);
            }
        }
    }

    public static class TestStartedExecutor {

        @Test
        public void testStartShouldBeInvoked() {
            IWindowedBolt bolt = mock(IWindowedBolt.class);
            WindowedBoltExecutor spiedExecutor = spy(new WindowedBoltExecutor(bolt));
            Map<String, Object> simpleConfiguration = new HashMap<>();
            simpleConfiguration.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 1);
            spiedExecutor.prepare(simpleConfiguration, mock(TopologyContext.class), mock(OutputCollector.class));
            verify(spiedExecutor, times(1)).start();
        }
    }
}
