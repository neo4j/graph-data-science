package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.RequestCorrelationId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.LoggerForProgressTracking;
import org.neo4j.gds.core.utils.progress.tasks.Progress;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProgressTrackerFactoryTest {
    @Mock
    private LoggerForProgressTracking log;
    @Mock
    private RequestCorrelationId requestCorrelationId;
    @Mock
    private TaskRegistryFactory taskRegistryFactory;
    @Mock
    private UserLogRegistryFactory userLogRegistryFactory;
    @Mock
    private Task task;
    @Mock
    private Progress progress;

    @Test
    void shouldCreateTaskProgressTrackerWhenLogProgressTrue() {
        var concurrency = new Concurrency(2);
        when(progress.volume()).thenReturn(10L);
        when(task.getProgress()).thenReturn(progress);

        var factory = new ProgressTrackerFactory(
            log,
            requestCorrelationId,
            taskRegistryFactory,
            userLogRegistryFactory
        );

        var tracker = factory.create(task, new JobId("jid-test"), concurrency, true);
        assertThat(tracker).isNotNull();
        tracker.requestedConcurrency(concurrency);
    }

    @Test
    void shouldCreateTaskTreeProgressTrackerWhenLogProgressFalse() {
        var concurrency = new Concurrency(4);
        when(progress.volume()).thenReturn(1L);
        when(task.getProgress()).thenReturn(progress);

        var factory = new ProgressTrackerFactory(
            log,
            requestCorrelationId,
            taskRegistryFactory,
            userLogRegistryFactory
        );

        var tracker = factory.create(task, new JobId("jid-test"), concurrency, false);
        assertThat(tracker).isNotNull();
        tracker.requestedConcurrency(concurrency);
    }

    @Test
    void shouldReturnNullTracker() {
        var factory = new ProgressTrackerFactory(
            log,
            requestCorrelationId,
            taskRegistryFactory,
            userLogRegistryFactory
        );

        var tracker = factory.nullTracker();
        assertThat(tracker).isSameAs(ProgressTracker.NULL_TRACKER);
    }
}
