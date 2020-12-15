/*
 * Copyright (c) 2017-2020 "Neo4j,"
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

import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ProgressEventConsumerTest {

    @Test
    void testConsumerLifecycle() {
        var username = AuthSubject.ANONYMOUS.username();
        var expectedEvents = new ArrayList<LogEvent>();

        var fakeClockScheduler = new FakeClockJobScheduler();
        var runner = Neo4jProxy.runnerFromScheduler(fakeClockScheduler, Group.TESTING);
        var queue = new ArrayBlockingQueue<LogEvent>(1);
        var consumer = new ProgressEventConsumer(runner, queue);

        // initial set is empty
        assertThat(consumer.query(username)).isEmpty();

        var event = ImmutableLogEvent.of("foo", "bar", 42.0);
        queue.add(event);

        // nothing polled yet
        assertThat(consumer.query(username)).isEmpty();

        consumer.start();

        // starting the component will trigger the initial polling
        expectedEvents.add(event);
        assertThat(consumer.query(username)).containsExactlyElementsOf(expectedEvents);

        // add another event
        event = ImmutableLogEvent.of("baz", "qux", 1337.0);
        queue.add(event);

        // nothing is polled ...
        assertThat(consumer.query(username)).containsExactlyElementsOf(expectedEvents);

        // ...until we advance the time
        expectedEvents.add(event);
        fakeClockScheduler.forward(100, TimeUnit.MILLISECONDS);
        assertThat(consumer.query(username)).containsExactlyElementsOf(expectedEvents);

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
