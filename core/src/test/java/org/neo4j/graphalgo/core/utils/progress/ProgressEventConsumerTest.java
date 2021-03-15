/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.core.utils.progress;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.internal.kernel.api.security.AuthSubject;
import org.neo4j.scheduler.Group;
import org.neo4j.test.FakeClockJobScheduler;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressEventConsumerTest {

    @Test
    void testConsumerLifecycle() {
        var username = AuthSubject.ANONYMOUS.username();

        var fakeClockScheduler = new FakeClockJobScheduler();
        var runner = Neo4jProxy.runnerFromScheduler(fakeClockScheduler, Group.TESTING);
        var queue = new ArrayBlockingQueue<LogEvent>(1);
        var consumer = new ProgressEventConsumer(runner, queue);

        // initial set is empty
        assertThat(consumer.query(username)).isEmpty();

        var event1 = ImmutableLogEvent.of(username, new JobId(), "foo", "bar", 42.0);
        queue.add(event1);

        // nothing polled yet
        assertThat(consumer.query(username)).isEmpty();

        consumer.start();

        // starting the component will trigger the initial polling
        assertThat(consumer.query(username)).containsExactly(event1);

        // add another event
        var event2 = ImmutableLogEvent.of(username, new JobId(), "baz", "qux", 1337.0);
        queue.add(event2);

        // nothing is polled ...
        assertThat(consumer.query(username)).containsExactly(event1);

        // ...until we advance the time
        fakeClockScheduler.forward(100, TimeUnit.MILLISECONDS);
        assertThat(consumer.query(username)).containsExactlyInAnyOrder(event1, event2);

        consumer.stop();
    }

    @Test
    void shouldKnowWhenEventStoreIsEmpty() {
        var username = AuthSubject.ANONYMOUS.username();

        var fakeClockScheduler = new FakeClockJobScheduler();
        var runner = Neo4jProxy.runnerFromScheduler(fakeClockScheduler, Group.TESTING);
        var queue = new ArrayBlockingQueue<LogEvent>(1);
        var consumer = new ProgressEventConsumer(runner, queue);

        var jobId = new JobId();

        // initial set is empty
        assertThat(consumer.isEmpty()).isTrue();

        var event1 = ImmutableLogEvent.of(username, jobId, "foo", "bar", 42.0);
        queue.add(event1);

        // nothing polled yet
        assertThat(consumer.isEmpty()).isTrue();

        consumer.start();

        // starting the component will trigger the initial polling
        assertThat(consumer.isEmpty()).isFalse();

        // add another event and advance time
        var event2 = ImmutableLogEvent.of(username, jobId, "baz", "qux", 1337.0);
        queue.add(event2);
        fakeClockScheduler.forward(100, TimeUnit.MILLISECONDS);

        // the store is still not empty
        assertThat(consumer.isEmpty()).isFalse();

        // add a terminal event and advance time
        var endOfStreamEvent = LogEvent.endOfStreamEvent(username, jobId);
        queue.add(endOfStreamEvent);
        fakeClockScheduler.forward(100, TimeUnit.MILLISECONDS);

        // and the queue should now be empty again
        assertThat(consumer.isEmpty()).isTrue();

        consumer.stop();
    }

    @Test
    void testConsumerStartStop() {
        var consumer = new ProgressEventConsumer(
            // empty runner that does nothing
            (runnable, initialDelay, rate, timeUnit) -> () -> {},
            new ArrayBlockingQueue<>(1)
        );

        assertThat(consumer.isRunning()).isFalse();

        consumer.start();
        assertThat(consumer.isRunning()).isTrue();

        consumer.stop();
        assertThat(consumer.isRunning()).isFalse();
    }
}
