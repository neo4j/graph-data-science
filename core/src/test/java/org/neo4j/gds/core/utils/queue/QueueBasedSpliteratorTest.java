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
package org.neo4j.gds.core.utils.queue;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.TerminationFlag;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;

class QueueBasedSpliteratorTest {

    @Test
    void shouldIterateAllElementsOfTheQueue() {
        var expected = List.of(0L, 1L, 2L);

        var queue = new ArrayBlockingQueue<Long>(10);
        queue.addAll(expected);
        queue.add(-1L);

        var spliterator = new QueueBasedSpliterator<>(queue, -1L, TerminationFlag.RUNNING_TRUE, 100);

        var actual = StreamSupport.stream(spliterator, false).collect(Collectors.toList());

        assertThat(actual).containsExactlyElementsOf(expected);
    }

    @Test
    void shouldTimeout() {
        var expected = List.of(0L, 1L, 2L);

        var queue = new ArrayBlockingQueue<Long>(10);
        queue.addAll(expected);

        var spliterator = new QueueBasedSpliterator<>(queue, -1L, TerminationFlag.RUNNING_TRUE, 1);

        var actual = StreamSupport.stream(spliterator, false).collect(Collectors.toList());

        assertThat(actual).containsExactlyElementsOf(expected);
    }

}
