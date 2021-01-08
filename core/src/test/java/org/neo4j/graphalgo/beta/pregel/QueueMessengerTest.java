/*
 * Copyright (c) 2017-2021 "Neo4j,"
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

import org.jctools.queues.MpscLinkedQueue;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueueMessengerTest {

    private static Stream<Arguments> iteratorTypes() {
        return Stream.of(
            Arguments.of(new QueueMessenger.QueueIterator.Async()),
            Arguments.of(new QueueMessenger.QueueIterator.Sync())
        );
    }

    @ParameterizedTest
    @MethodSource("iteratorTypes")
    void allowMultipleCallsToHasNext(QueueMessenger.QueueIterator messageIterator) {
        var queue = new MpscLinkedQueue<Double>();
        queue.offer(42.0);
        if (messageIterator instanceof QueueMessenger.QueueIterator.Sync) {
            queue.offer(Double.NaN);
        }

        messageIterator.init(queue);

        assertTrue(messageIterator.hasNext());
        assertTrue(messageIterator.hasNext());

        assertEquals(42.0, messageIterator.next());

        assertFalse(messageIterator.hasNext());
        assertFalse(messageIterator.hasNext());
    }
}
