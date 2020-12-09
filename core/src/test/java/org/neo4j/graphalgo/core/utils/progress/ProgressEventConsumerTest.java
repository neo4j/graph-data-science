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
import org.neo4j.internal.kernel.api.security.AuthSubject;

import java.util.concurrent.ArrayBlockingQueue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class ProgressEventConsumerTest {

    @Test
    void testConsumer() {
        var username = AuthSubject.ANONYMOUS.username();
        var queue = new ArrayBlockingQueue<LogEvent>(1);
        var consumer = new ProgressEventConsumer(queue);

        // nothing in there
        assertThat(consumer.query(username)).isEmpty();

        var event = ImmutableLogEvent.of("foo", "bar", 42.0);
        queue.offer(event);

        // nothing polled yet
        assertThat(consumer.query(username)).isEmpty();

        consumer.pollNext();
        assertThat(consumer.query(username)).containsExactly(event);

        var event2 = ImmutableLogEvent.of("baz", "qux", 1337.0);
        queue.offer(event2);

        consumer.pollNext();
        assertThat(consumer.query(username)).containsExactly(event, event2);
    }

    @Test
    void testConsumerLifecycle() throws InterruptedException {
        var username = AuthSubject.ANONYMOUS.username();
        var queue = new ArrayBlockingQueue<LogEvent>(1);
        var consumer = new ProgressEventConsumer(queue);

        assertThat(consumer.query(username)).isEmpty();

        var event = ImmutableLogEvent.of("foo", "bar", 42.0);
        queue.add(event);

        // nothing polled yet
        assertThat(consumer.query(username)).isEmpty();

        consumer.start();

        Thread.sleep(1000);
        assertThat(consumer.query(username)).containsExactly(event);

        var event2 = ImmutableLogEvent.of("baz", "qux", 1337.0);
        // wait for the queue to be empty, i.e. the element has been polled
        queue.add(event2);

        Thread.sleep(1000);

        // same dance
        queue.put(event2);
        assertThat(consumer.query(username)).containsExactly(event, event2);

        consumer.stop();
    }
}
