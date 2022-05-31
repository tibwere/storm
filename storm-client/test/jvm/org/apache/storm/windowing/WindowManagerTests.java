package org.apache.storm.windowing;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Test cases for WindowManager class
 *
 * @author Simone Tiberi
 */
@RunWith(value = Enclosed.class)
public class WindowManagerTests {
    /* immediately deliver events */
    private static final int TRIGGER_WINDOW = 1;

    @RunWith(Parameterized.class)
    public static class SimpleEventTests {
        private WindowManager<String> windowManager;
        private int threshold;
        private List<String> eventsToBeAdded;
        private List<String> actualExpiredEvents;

        private List<String> expectedExpiredEvents;

        public SimpleEventTests(int threshold, List<String> eventsToBeAdded) {
            configure(threshold, eventsToBeAdded);
        }

        public void configure(int threshold, List<String> eventsToBeAdded) {
            this.threshold = threshold;
            this.eventsToBeAdded = eventsToBeAdded;
        }

        /**
         * BOUNDARY VALUE ANALYSIS
         *  - threshold:            [list_size -1, list_size + 1]
         *  - eventsToBeAdded:      [empty-list, non-empty-list]
         */
        @Parameterized.Parameters
        public static Collection<Object[]> testCasesTuples() {

            List<String> eventsList = Arrays.asList("isw2", "exam", "sw", "testing");

            return Arrays.asList(new Object[][]{
                    {1, Collections.emptyList()},
                    {3, eventsList},
                    {5, eventsList}
            });
        }

        @Before
        public void setUpEnvironment() {
            this.setExpectedExpiredEvents();
            this.actualExpiredEvents = new ArrayList<>();
            WindowLifecycleListener<String> listener = mock(WindowLifecycleListener.class);
            doAnswer((mockedInstance) -> {
                List<String> expiredEvents = mockedInstance.getArgument(0);
                this.actualExpiredEvents.addAll(expiredEvents);
                return null;
            }).when(listener).onExpiry(anyList());
            this.windowManager = new WindowManager<>(listener);
            CountEvictionPolicy<String> evictionPolicy = new CountEvictionPolicy<>(this.threshold);
            CountTriggerPolicy<String> triggerPolicy = new CountTriggerPolicy<>(TRIGGER_WINDOW, this.windowManager,
                    evictionPolicy);
            this.windowManager.setEvictionPolicy(evictionPolicy);
            this.windowManager.setTriggerPolicy(triggerPolicy);
            triggerPolicy.start();
        }

        @After
        public void tearDownEnvironment() {
            this.windowManager.shutdown();
        }

        @Test
        public void testInteraction() {
            for (String event : this.eventsToBeAdded)
                this.windowManager.add(event);

            Assert.assertEquals("Wrong eviction set", this.expectedExpiredEvents, this.actualExpiredEvents);
        }

        /**
         * this method evaluates the list of evicted tuples
         */
        private void setExpectedExpiredEvents() {
            if (this.threshold > this.eventsToBeAdded.size()) {
                this.expectedExpiredEvents = Collections.emptyList();
            } else {
                int expiredLength = this.eventsToBeAdded.size() - this.threshold;
                this.expectedExpiredEvents = new ArrayList<>();
                for (int i = 0; i < expiredLength; ++i)
                    this.expectedExpiredEvents.add(this.eventsToBeAdded.get(i));
            }
        }
    }

    public static class TestWatermarkEvent {

        @Test
        public void testWEShouldNotBeEnqueued() {
            WindowManager<String> windowManager = new WindowManager<>(mock(WindowLifecycleListener.class));
            CountEvictionPolicy<String> evictionPolicy = new CountEvictionPolicy<>(5);
            CountTriggerPolicy<String> triggerPolicy = new CountTriggerPolicy<>(TRIGGER_WINDOW, windowManager,
                    evictionPolicy);
            windowManager.setEvictionPolicy(evictionPolicy);
            windowManager.setTriggerPolicy(triggerPolicy);

            windowManager.add(new WaterMarkEvent<>(10));
            Assert.assertTrue("A watermark event should not be enqueued",
                    windowManager.queue.isEmpty());
        }
    }

    public static class TestCompaction {
        @Test
        public void testAdditionShouldTriggerCompaction() {
            WindowManager<String> windowManager = spy(new WindowManager<>(mock(WindowLifecycleListener.class)));
            CountEvictionPolicy<String> evictionPolicy = new CountEvictionPolicy<>(5);
            windowManager.setEvictionPolicy(evictionPolicy);
            windowManager.setTriggerPolicy(new CountTriggerPolicy<>(TRIGGER_WINDOW, windowManager, evictionPolicy));

            windowManager.add("test-addition");
            verify(windowManager, times(1)).compactWindow();
        }
    }

}
