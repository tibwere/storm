package org.apache.storm.windowing;

import org.apache.storm.Config;
import org.apache.storm.topology.WindowedBoltExecutorTests;
import org.apache.storm.tuple.Tuple;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(value = Enclosed.class)
public class WindowedBoltExecutorWMRelatedTests {

    @RunWith(Parameterized.class)
    public static class StandardInitScenariosTests {
        private Map<String, Object> configurations;
        private WindowManager<Tuple> windowManager;
        private String interestingKey;
        private String prefix;
        private PolicyType policyType;

        public StandardInitScenariosTests(Map<String, Object> configurations, String interestingKey,
                                               String prefix, PolicyType policyType) {
            configure(configurations, interestingKey, prefix, policyType);
        }

        public void configure(Map<String, Object> configurations, String interestingKey, String prefix,
                              PolicyType policyType) {
            this.configurations = configurations;
            this.interestingKey = interestingKey;
            this.prefix = prefix;
            this.policyType = policyType;
            this.windowManager = WindowedBoltExecutorTests.getPreparedWindowManager(this.configurations,
                    true, false);
        }

        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {

            Map<String, Object> wldSpecified = new HashMap<>();
            // duration must be higher than window length
            wldSpecified.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 15);
            wldSpecified.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 10);

            Map<String, Object> sicSpecified = new HashMap<>();
            // window length must be specified in count scenario
            sicSpecified.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 1);
            sicSpecified.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, 2);

            Map<String, Object> sidSpecified = new HashMap<>();
            // window length must be specified in count scenario and
            // duration must be higher than window length
            sidSpecified.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 15);
            sidSpecified.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 10);
            sidSpecified.put(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, 5);

            Map<String, Object> ttmlSpecified = new HashMap<>();
            ttmlSpecified.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 15);
            ttmlSpecified.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS, 10);
            ttmlSpecified.put(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS, 10);

            return Arrays.asList(new Object[][]{
                    // CONFIGURATION    INTERESTING_KEY                                     PREFIX                  POLICY_TYPE
                    {  wldSpecified,    Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS,    "windowLength=",        PolicyType.EVICTION},
                    {  sicSpecified,    Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT,       "count=",               PolicyType.TRIGGER},
                    {  sidSpecified,    Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS, "slidingIntervalMs=",   PolicyType.TRIGGER},
                    {  ttmlSpecified,   Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS,   "lag=",                 PolicyType.EVICTION}
            });
        }

        @Test
        public void testWMConfiguration() {

            String expectedContainedString = this.prefix + this.configurations.get(this.interestingKey);
            String actualToStringValue = (PolicyType.EVICTION.equals(this.policyType)) ?
                    this.windowManager.evictionPolicy.toString() :
                    this.windowManager.triggerPolicy.toString();

            System.out.println(actualToStringValue);

            Assert.assertTrue("Wrong window manager initialization",
                    actualToStringValue.contains(expectedContainedString));
        }
    }

    public static class OtherScenariosTests {

        /* for prepare */
        @Test
        public void testLateStreamWithoutExtractor() {
            try {
                Map<String, Object> lateStreamSpecified = new HashMap<>();
                lateStreamSpecified.put(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, "test-stream");
                WindowedBoltExecutorTests.getPreparedWindowManager(
                        lateStreamSpecified,false, false);
                Assert.fail("This configuration should not be correct");
            } catch (IllegalArgumentException e) {
                Assert.assertTrue(true);
            }
        }

        @Test
        public void testStatefulWM() {
            Map<String, Object> validConfig = new HashMap<>();
            validConfig.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, 1);

            WindowManager<Tuple> windowManager = WindowedBoltExecutorTests.getPreparedWindowManager(validConfig,
                    false, true);
            Assert.assertTrue("The WM should be stateful",
                    (windowManager instanceof StatefulWindowManager));
        }

        private void testAddition(WindowManager wm) {
            Assert.assertEquals("The tuple should be enqueued", 1, wm.queue.size());
        }

        /* for execute */
        @Test
        public void testNoTSExtractorAddition() {
            testAddition(WindowedBoltExecutorTests.getPreparedAndExecutedWM(false, null));
        }

        @Test
        public void testInTimeTupleAddition() {
            WaterMarkEventGenerator waterMarkEventGenerator = mock(WaterMarkEventGenerator.class);
            when(waterMarkEventGenerator.track(any(), anyLong())).thenReturn(true);
            testAddition(WindowedBoltExecutorTests.getPreparedAndExecutedWM(true, waterMarkEventGenerator));
        }
    }

    private enum PolicyType { EVICTION, TRIGGER }
}
