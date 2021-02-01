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
package org.neo4j.graphalgo.beta.pregel;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class PrimitiveDoubleQueuesTest {

    @Test
    void growQueueArray() {
        var queues = new PrimitiveDoubleQueues(
            1337,
            42,
            AllocationTracker.empty()
        );

        assertThat(queues.queue(42).length).isEqualTo(42);

        for (int i = 0; i < 41; i++) {
            queues.push(42, 23);
        }

        assertThat(queues.queue(42).length).isEqualTo(42);
        queues.push(42, 1337);
        assertThat(queues.queue(42).length).isEqualTo(42);
        queues.push(42, 1337);

        assertThat(queues.queue(42).length).isEqualTo(63 /* 42 * 1.5 */);
        assertThat(queues.queue(42)[41]).isEqualTo(1337);
        assertThat(queues.queue(42)[42]).isEqualTo(1337);

        queues.push(42, 1337);
        assertThat(queues.queue(42)[43]).isEqualTo(1337);
    }
}
